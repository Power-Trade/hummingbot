#!/usr/bin/env python
import json

import aiohttp
import asyncio
import logging
import math
import pandas as pd
import ujson

from decimal import Decimal
from typing import (
    Dict,
    List,
    Optional,
    Any,
    AsyncIterable,
)

from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.exchange.powertrade import powertrade_constants as CONSTANTS
from hummingbot.connector.exchange.powertrade import powertrade_utils
from hummingbot.connector.exchange.powertrade.powertrade_websocket_adaptor import PowertradeWebSocketAdaptor
from hummingbot.connector.exchange.powertrade.powertrade_auth import PowertradeAuth
from hummingbot.connector.exchange.powertrade.powertrade_in_flight_order import PowertradeInFlightOrder
from hummingbot.connector.exchange.powertrade.powertrade_order_book_tracker import PowertradeOrderBookTracker
from hummingbot.connector.exchange.powertrade.powertrade_user_stream_tracker import PowertradeUserStreamTracker
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.common import OpenOrder
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.event.events import (
    MarketEvent,
    BuyOrderCompletedEvent,
    SellOrderCompletedEvent,
    OrderFilledEvent,
    OrderCancelledEvent,
    BuyOrderCreatedEvent,
    SellOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderType,
    TradeType,
    TradeFee
)
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils import async_ttl_cache
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather, safe_wrapper
from hummingbot.logger import HummingbotLogger
from hummingbot.model.credit_limit import CreditLimit

powertrade_logger = None
s_decimal_NaN = Decimal("nan")


class PowertradeExchange(ExchangeBase):
    """
    PowertradeExchange connects with Powertrade exchange and provides order book pricing, user account tracking and
    trading functionality.
    """
    API_CALL_TIMEOUT = 10.0
    SHORT_POLL_INTERVAL = 5.0
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    LONG_POLL_INTERVAL = 120.0
    HISTORY_RECON_INTERVAL = 3600.0
    TRADING_RULES_POLL_INTERVAL = 60.0

    DAY_IN_MICROSECONDS = 8.64e+10
    HOUR_IN_MICROSECONDS = 3600 * 1e6

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global powertrade_logger
        if powertrade_logger is None:
            powertrade_logger = logging.getLogger(__name__)
        return powertrade_logger

    def __init__(self,
                 powertrade_api_key: str,
                 powertrade_secret_key: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain="prod",
                 uri=None
                 ):
        """
        :param powertrade_api_key: The API key to connect to private Powertrade APIs.
        :param powertrade_secret_key: The API secret.
        :param trading_pairs: The market trading pairs which to track order book data.
        :param trading_required: Whether actual trading is needed.
        """
        self._domain = domain
        self._uri = uri or CONSTANTS.uri[self._domain]
        super().__init__()
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._powertrade_auth = PowertradeAuth(powertrade_api_key, powertrade_secret_key, self._uri)
        self._order_book_tracker = PowertradeOrderBookTracker(trading_pairs=trading_pairs, domain=domain)
        self._user_stream_tracker = PowertradeUserStreamTracker(trading_pairs, domain=domain)
        self._ws_adaptor = PowertradeWebSocketAdaptor(trackers=[self._order_book_tracker, self._user_stream_tracker],
                                                      ws_auth=self._powertrade_auth, domain=self._domain)
        self._ev_loop = asyncio.get_event_loop()
        self._shared_client = None
        self._poll_notifier = asyncio.Event()
        self._last_timestamp = 0
        self._in_flight_orders = {}  # Dict[client_order_id:str, PowertradeInFlightOrder]
        self._order_not_found_records = {}  # Dict[client_order_id:str, count:int]
        self._trading_rules = {}  # Dict[trading_pair:str, TradingRule]
        self._symbol_to_tradeable_entity_id_map = {}    # Dict[trading_pair:str, tradeable_entity_id: int]
        self._last_poll_timestamp = 0
        self._last_trade_history_request_timestamp = 0

        self._status_polling_task = None
        self._user_stream_tracker_task = None
        self._user_stream_event_listener_task = None
        self._trading_rules_polling_task = None
        self._ws_adaptor_task = None
        self.check_network_timeout = 60
        self.limit = {}
        if trading_pairs:
            for symbol in trading_pairs:
                base = symbol.split('-')[0]
                term = symbol.split('-')[1]
                credit_limit = CreditLimit.get_record(self.sql.get_shared_session(), self.display_name, base, term)
                if not credit_limit:
                    CreditLimit.update_volume(self.sql.get_shared_session(), self.display_name, base , term, base_volume=Decimal("0.0"), quote_volume=Decimal("0.0"),
                                              base_limit=Decimal("0.0"), quote_limit=Decimal("0.0"))
                    credit_limit = CreditLimit.get_record(self.sql.get_shared_session(), self.display_name, base, term)
                self.limit[base] = Decimal(str(credit_limit.base_limit))
                self.limit[term] = Decimal(str(credit_limit.quote_limit))

    @property
    def name(self) -> str:
        if self._domain == "prod":
            return CONSTANTS.EXCHANGE_NAME
        else:
            return f"{CONSTANTS.EXCHANGE_NAME}_{self._domain}"

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._order_book_tracker.order_books

    @property
    def in_flight_orders(self) -> Dict[str, PowertradeInFlightOrder]:
        return self._in_flight_orders

    @property
    def trading_rules(self) -> Dict[str, TradingRule]:
        return self._trading_rules

    @property
    def symbol_to_tradeable_entity_id_map(self) -> Dict[str, str]:
        return self._symbol_to_tradeable_entity_id_map

    @property
    def tradeable_entity_id_to_symbol_map(self):
        return {v: k for k, v in self._symbol_to_tradeable_entity_id_map.items()}

    @property
    def status_dict(self) -> Dict[str, bool]:
        """
        A dictionary of statuses of various connector's components.
        """
        return {
            "order_books_initialized": self._order_book_tracker.ready,
            "account_balance": len(self._account_balances) > 0 if self._trading_required else True,
            "trading_rule_initialized": len(self._trading_rules) > 0,
            "user_stream_initialized": self._user_stream_tracker.ready if self._trading_required else True,
        }

    @property
    def ready(self) -> bool:
        """
        :return True when all statuses pass, this might take 5-10 seconds for all the connector's components and
        services to be ready.
        """
        return all(self.status_dict.values())

    @property
    def limit_orders(self) -> List[LimitOrder]:
        return [
            in_flight_order.to_limit_order()
            for in_flight_order in self._in_flight_orders.values()
        ]

    @property
    def tracking_states(self) -> Dict[str, any]:
        """
        :return active in-flight orders in json format, is used to save in sqlite db.
        """
        return {
            key: value.to_json()
            for key, value in self._in_flight_orders.items()
            if not value.is_done
        }

    def restore_tracking_states(self, saved_states: Dict[str, any]):
        """
        Restore in-flight orders from saved tracking states, this is st the connector can pick up on where it left off
        when it disconnects.
        :param saved_states: The saved tracking_states.
        """
        self._in_flight_orders.update({
            key: PowertradeInFlightOrder.from_json(value)
            for key, value in saved_states.items()
        })

    def supported_order_types(self) -> List[OrderType]:
        """
        :return a list of OrderType supported by this connector.
        Note that Market order type is no longer required and will not be used.
        """
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    def start(self, clock: Clock, timestamp: float):
        """
        This function is called automatically by the clock.
        """
        super().start(clock, timestamp)

    def stop(self, clock: Clock):
        """
        This function is called automatically by the clock.
        """
        super().stop(clock)

    async def start_network(self):
        """
        This function is required by NetworkIterator base class and is called automatically.
        It starts tracking order book, polling trading rules,
        updating statuses and tracking user data.
        """

        self.logger().info("Invoking start_network")
        try:
            await self.stop_network()
            self._ws_adaptor_task = safe_ensure_future(self._ws_adaptor.websocket_main_update_loop())
            self._order_book_tracker.start()
            self._trading_rules_polling_task = safe_ensure_future(self._trading_rules_polling_loop())
            if self._trading_required:
                self._status_polling_task = safe_ensure_future(self._status_polling_loop())
                self._user_stream_tracker_task = safe_ensure_future(self._user_stream_tracker.start())
                self._user_stream_event_listener_task = safe_ensure_future(self._user_stream_event_listener())
        except Exception as e:
            self.logger().error(f"Failed whilst calling start_network\r\n{str(e)}", exc_info=True)
            raise

    async def stop_network(self):
        """
        This function is required by NetworkIterator base class and is called automatically.
        """
        try:
            self._order_book_tracker.stop()
            if self._ws_adaptor_task is not None:
                self._ws_adaptor_task.cancel()
                self._ws_adaptor_task = None
            if self._status_polling_task is not None:
                self._status_polling_task.cancel()
                self._status_polling_task = None
            if self._trading_rules_polling_task is not None:
                self._trading_rules_polling_task.cancel()
                self._trading_rules_polling_task = None
            if self._user_stream_tracker_task is not None:
                self._user_stream_tracker_task.cancel()
                self._user_stream_tracker_task = None
            if self._user_stream_event_listener_task is not None:
                self._user_stream_event_listener_task.cancel()
                self._user_stream_event_listener_task = None
        except Exception as e:
            self.logger().error(f"Failed whilst calling stop_network\r\n{str(e)}", exc_info=True)
            raise

    async def check_network(self) -> NetworkStatus:
        """
        This function is required by NetworkIterator base class and is called periodically to check
        the network connection. Simply ping the network (or call any light weight public API).
        """
        try:
            # since there is no ping endpoint, the lowest rate call is to get time
            resp = await self._api_request(
                method="GET",
                path_url=CONSTANTS.TIME_URL(url=CONSTANTS.rest_url[self._domain])
            )
            if "server_utc_timestamp" not in resp:
                raise Exception("Malformed /time response")
        except asyncio.CancelledError as ce:
            self.logger().exception(f"cancel error {str(ce)}")
            raise
        except aiohttp.client.ServerDisconnectedError as se:
            self.logger().exception(f"server disconnected {str(se)}")
        except Exception as ex:
            self.logger().exception(f"weird error {str(ex)}")
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED

    async def _http_client(self) -> aiohttp.ClientSession:
        """
        :returns Shared client session instance
        """
        if self._shared_client is None:
            self._shared_client = aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False))
        return self._shared_client

    async def _trading_rules_polling_loop(self):
        """
        Periodically update trading rule.
        """
        while True:
            try:
                await self._update_trading_rules()
                await asyncio.sleep(self.TRADING_RULES_POLL_INTERVAL)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().network(f"Unexpected error while fetching trading rules. Error: {str(e)}",
                                      exc_info=True,
                                      app_warning_msg="Could not fetch new trading rules from Powertrade. "
                                                      "Check network connection.")
                await asyncio.sleep(0.5)

    async def _update_trading_rules(self):
        market_info = await self._api_request(
            method="GET",
            path_url=CONSTANTS.ENTITIES_AND_RULES_URL(url=CONSTANTS.rest_url[self._domain])
        )
        self._trading_rules.clear()
        self._symbol_to_tradeable_entity_id_map.clear()
        self._trading_rules, self._symbol_to_tradeable_entity_id_map = self._format_trading_rules(market_info)

    @async_ttl_cache(ttl=600)   # Refresh period = 10 min
    async def get_deliverable_id_to_symbol_map(self):
        deliverables_info = await self._api_request(
            method="GET",
            path_url=CONSTANTS.DELIVERABLES_URL(url=CONSTANTS.rest_url[self._domain])
        )
        deliverable_id_to_symbol_map = self._format_deliverable_ids_map(deliverables_info)
        return deliverable_id_to_symbol_map

    def _format_deliverable_ids_map(self, deliverables_info):
        deliverable_id_to_symbol_map = {}
        try:
            deliverable_info = deliverables_info['deliverables_response']
            for deliverable in deliverable_info['deliverables']:
                try:
                    symbol = deliverable['symbol']
                    deliverable_id = deliverable['deliverable_id']
                    deliverable_id_to_symbol_map[deliverable_id] = symbol
                except Exception:
                    continue
        except Exception:
            self.logger().error("Error parsing the deliverable ids.", exc_info=True)
        return deliverable_id_to_symbol_map

    def _format_trading_rules(self, market_info: Dict[str, Any]) -> Dict[str, TradingRule]:
        """
        Converts json API response into a dictionary of trading rules.
        :param market_info: The json API response
        :return A dictionary of trading rules.
        Response Example:
        {
        "entities_and_rules_response": {
            "timezone": "UTC",
            "server_timestamp": "1627658751452827",
            "user_tag": "none",
            "more_flag": "false",
            "symbols": [
                {
                    "symbol": "BTC-20210820-20000P",
                    "status": "active",
                    "base_asset": "BTC-20210820-20000P",
                    "base_asset_precision": "8",
                    "quote_asset": "USDT",
                    "quote_asset_precision": "10",
                    "quote_precision": "2"
                },
                ...
        """
        trading_rules = {}
        symbol_to_tradeable_entity_id_map = {}
        try:
            market_info = market_info["entities_and_rules_response"]
            for market in market_info["symbols"]:
                try:
                    trading_pair = market["symbol"]
                    trading_rules[trading_pair] = TradingRule(
                        trading_pair=trading_pair,
                        min_order_size=Decimal(str(market["minimum_quantity"])),
                        max_order_size=Decimal(str(market["maximum_quantity"])),
                        min_order_value=Decimal(str(market["minimum_value"])),
                        min_price_increment=Decimal(str(market["price_step"])),
                        min_base_amount_increment=Decimal(str(market["quantity_step"])),
                        min_quote_amount_increment=Decimal(str(market["price_step"])))
                    symbol_to_tradeable_entity_id_map[trading_pair] = market["tradeable_entity_id"]
                except Exception:
                    continue
                    # self.logger().error(f"Error parsing the trading pair rule {market}. Skipping.", exc_info=True)
        except Exception:
            self.logger().error("Error parsing the trading pair rules.", exc_info=True)
        return trading_rules, symbol_to_tradeable_entity_id_map

    async def _api_request(self,
                           method: str,
                           path_url: str,
                           params: Optional[Dict[str, Any]] = None,
                           data: Optional[Dict[str, Any]] = None,
                           is_auth_required: bool = False) -> Dict[str, Any]:
        """
        Sends an aiohttp request and waits for a response.
        :param method: The HTTP method, e.g. get or post
        :param path_url: The path url or the API end point
        :param is_auth_required: Whether an authentication is required, when True the function will add encrypted
        signature to the request.
        :returns A response in json format.
        """
        path_url = path_url.format(self._domain)
        start_time = int(pd.Timestamp.utcnow().timestamp())
        async with aiohttp.ClientSession() as client:
            try:
                if is_auth_required:
                    headers = self._powertrade_auth.get_auth_headers()  # Maybe in the future the client will be needed?
                else:
                    headers = self._powertrade_auth.get_headers()

                if method == "GET":
                    response = await client.get(path_url, headers=headers, params=params)
                elif method == "POST":
                    response = await client.post(path_url, headers=headers, data=ujson.dumps(data), params=params)
                elif method == "DELETE":
                    response = await client.delete(path_url, headers=headers, data=ujson.dumps(data), params=params)
                else:
                    raise NotImplementedError(f"{method} HTTP Method not implemented. ")

                parsed_response = await response.json()

                if is_auth_required and self._powertrade_auth.is_auth_failed_message(parsed_response):
                    self._powertrade_auth.reset_credentials_expiration_timer()
                    self.logger().error("Authorization failed. Recycling auth ")
                    raise Exception(f"Authorization failed. {parsed_response} "
                                    f"Params: {params} "
                                    f"Data: {data}")

            except ValueError as e:
                self.logger().error(f"{str(e)}")
                raise ValueError(f"Error authenticating request {method} {path_url}. Error: {str(e)}")
            except aiohttp.client.ServerDisconnectedError as se:
                self.logger().info(f"server disconnect due to 20s has passed Duration - {int(pd.Timestamp.utcnow().timestamp()) - start_time}")
                raise se
            except Exception as e:
                self.logger().exception(e)
                raise IOError(f"Error parsing data from {path_url} , Duration - {int(pd.Timestamp.utcnow().timestamp()) - start_time}. Error: {str(e)}")
            if response.status != 200:
                raise IOError(f"Error fetching data from {path_url}. HTTP status is {response.status}. "
                              f"Message: {parsed_response} "
                              f"Params: {params} "
                              f"Data: {data}")

            return parsed_response

    def get_order_price_quantum(self, trading_pair: str, price: Decimal):
        """
        Returns a price step, a minimum price increment for a given trading pair.
        """
        trading_rule = self._trading_rules[trading_pair]
        return trading_rule.min_price_increment

    def get_order_size_quantum(self, trading_pair: str, order_size: Decimal):
        """
        Returns an order amount step, a minimum amount increment for a given trading pair.
        """
        trading_rule = self._trading_rules[trading_pair]
        return Decimal(trading_rule.min_base_amount_increment)

    def get_order_book(self, trading_pair: str) -> OrderBook:
        if trading_pair not in self._order_book_tracker.order_books:
            raise ValueError(f"No order book exists for '{trading_pair}'.")
        return self._order_book_tracker.order_books[trading_pair]

    def buy(self, trading_pair: str, amount: Decimal, order_type=OrderType.MARKET,
            price: Decimal = s_decimal_NaN, **kwargs) -> str:
        """
        Buys an amount of base asset (of the given trading pair). This function returns immediately.
        To see an actual order, you'll have to wait for BuyOrderCreatedEvent.
        :param trading_pair: The market (e.g. BTC-USDT) to buy from
        :param amount: The amount in base token value
        :param order_type: The order type
        :param price: The price (note: this is no longer optional)
        :returns A new internal order id
        """
        order_id: str = powertrade_utils.get_new_client_order_id(True, trading_pair)
        safe_ensure_future(self._create_order(TradeType.BUY, order_id, trading_pair, amount, order_type, price))
        return order_id

    def sell(self, trading_pair: str, amount: Decimal, order_type=OrderType.MARKET,
             price: Decimal = s_decimal_NaN, **kwargs) -> str:
        """
        Sells an amount of base asset (of the given trading pair). This function returns immediately.
        To see an actual order, you'll have to wait for SellOrderCreatedEvent.
        :param trading_pair: The market (e.g. BTC-USDT) to sell from
        :param amount: The amount in base token value
        :param order_type: The order type
        :param price: The price (note: this is no longer optional)
        :returns A new internal order id
        """
        order_id: str = powertrade_utils.get_new_client_order_id(False, trading_pair)
        safe_ensure_future(self._create_order(TradeType.SELL, order_id, trading_pair, amount, order_type, price))
        return order_id

    def cancel(self, trading_pair: str, order_id: str):
        """
        Cancel an order. This function returns immediately.
        To get the cancellation result, you'll have to wait for OrderCancelledEvent.
        :param trading_pair: The market (e.g. BTC-USDT) of the order.
        :param order_id: The internal order id (also called client_order_id)
        """
        safe_ensure_future(self._execute_cancel(trading_pair, order_id))
        return order_id

    async def _create_order(self,
                            trade_type: TradeType,
                            order_id: str,
                            trading_pair: str,
                            amount: Decimal,
                            order_type: OrderType,
                            price: Decimal):
        """
        Calls create-order API end point to place an order, starts tracking the order and triggers order created event.
        :param trade_type: BUY or SELL
        :param order_id: Internal order id (also called client_order_id)
        :param trading_pair: The market to place order
        :param amount: The order amount (in base token value)
        :param order_type: The order type
        :param price: The order price
        """
        if not order_type.is_limit_type():
            raise Exception(f"Unsupported order type: {order_type}")
        trading_rule = self._trading_rules[trading_pair]

        amount = self.quantize_order_amount(trading_pair, amount)
        price = self.quantize_order_price(trading_pair, price)

        try:
            if amount < trading_rule.min_order_size:
                raise ValueError(f"{trade_type.name} order amount {amount} is lower than the minimum order size "
                                 f"{trading_rule.min_order_size}.")

            order_value: Decimal = amount * price
            if order_value < trading_rule.min_order_value:
                raise ValueError(f"{trade_type.name} order value {order_value} is lower than the minimum order value "
                                 f"{trading_rule.min_order_value}")

            if trading_pair not in self._symbol_to_tradeable_entity_id_map:
                raise Exception(f"Missing {trading_pair} in map of trading pairs.")

            params = {
                "tradeable_entity_id": self._symbol_to_tradeable_entity_id_map.get(trading_pair),
                "type": "LIMIT",  # Powertrade Order Types ["limit", "market"}
                "side": trade_type.name.lower(),  # Powertrade Order Sides ["buy", "sell"]
                "time_in_force": "GTC",  # gtc = Good-Til-Cancelled
                "price": str(price),
                "quantity": str(amount),
                "client_order_id": order_id,
                "recv_window": '144',  # 144 cycles of 10 minutes = 1 day,
                "timestamp": str(int(self.current_timestamp)),
            }

            self.start_tracking_order(order_id,
                                      None,
                                      trading_pair,
                                      trade_type,
                                      price,
                                      amount,
                                      order_type
                                      )
            order_result = await self._api_request(
                method="POST",
                path_url=CONSTANTS.NEW_ORDER_URL(url=CONSTANTS.rest_url[self._domain]),
                params=params,
                is_auth_required=True
            )
            if "order_accepted" not in order_result:
                raise Exception(f"{order_result}")

            exchange_order_id = str(order_result["order_accepted"]["order_id"])
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is not None:
                self.logger().info(f"Created {order_type.name} {trade_type.name} order {order_id} for "
                                   f"{amount} {trading_pair}.")
                tracked_order.update_exchange_order_id(exchange_order_id)

            event_tag = MarketEvent.BuyOrderCreated if trade_type is TradeType.BUY else MarketEvent.SellOrderCreated
            event_class = BuyOrderCreatedEvent if trade_type is TradeType.BUY else SellOrderCreatedEvent
            self.trigger_event(event_tag,
                               event_class(
                                   self.current_timestamp,
                                   order_type,
                                   trading_pair,
                                   amount,
                                   price,
                                   order_id,
                                   exchange_order_id
                               ))
        except asyncio.CancelledError:
            raise
        except aiohttp.client.ServerDisconnectedError:
            self.logger().warning(f"{order_id} Server disconnect")
            event_tag = MarketEvent.BuyOrderCreated if trade_type is TradeType.BUY else MarketEvent.SellOrderCreated
            event_class = BuyOrderCreatedEvent if trade_type is TradeType.BUY else SellOrderCreatedEvent
            self.trigger_event(event_tag,
                               event_class(
                                   self.current_timestamp,
                                   order_type,
                                   trading_pair,
                                   amount,
                                   price,
                                   order_id
                               ))
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order:
                tracked_order.last_state = "not_found"
        except Exception as e:
            self.stop_tracking_order(order_id)
            self.logger().network(
                f"Error submitting {trade_type.name} {order_type.name} order to Powertrade for "
                f"{amount} {trading_pair} "
                f"{price}.",
                exc_info=True,
                app_warning_msg=str(e)
            )
            self.trigger_event(MarketEvent.OrderFailure,
                               MarketOrderFailureEvent(self.current_timestamp, order_id, order_type))

    def start_tracking_order(self,
                             order_id: str,
                             exchange_order_id: str,
                             trading_pair: str,
                             trade_type: TradeType,
                             price: Decimal,
                             amount: Decimal,
                             order_type: OrderType):
        """
        Starts tracking an order by simply adding it into _in_flight_orders dictionary.
        """
        self._in_flight_orders[order_id] = PowertradeInFlightOrder(
            client_order_id=order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=trading_pair,
            order_type=order_type,
            trade_type=trade_type,
            price=price,
            amount=amount
        )

    def stop_tracking_order(self, order_id: str):
        """
        Stops tracking an order by simply removing it from _in_flight_orders dictionary.
        """
        if order_id in self._in_flight_orders:
            del self._in_flight_orders[order_id]

    async def _execute_cancel(self, trading_pair: str, order_id: str) -> str:
        """
        Executes order cancellation process by first calling cancel-order API. The API result doesn't confirm whether
        the cancellation is successful, it simply states it receives the request.
        :param trading_pair: The market trading pair
        :param order_id: The internal order id
        order.last_state to change to CANCELED
        """
        try:
            body_params = {}
            tracked_order = self._in_flight_orders.get(order_id)
            ex_order_id = await tracked_order.get_exchange_order_id()
            if not self._in_flight_orders.get(order_id):
                self.logger().info(f"the order {order_id} might be already filled or cancel before")
                return order_id
            self.logger().info(f"exchange order id at this state {order_id} {ex_order_id}")
            if not ex_order_id:
                return order_id
            body_params = {
                "order_id": ex_order_id,
                "timestamp": str(int(self.current_timestamp)),
            }
            self.logger().info(f"starting cancel order {order_id}")

            await self._api_request(
                method="DELETE",
                path_url=CONSTANTS.CANCEL_ORDER_URL(url=CONSTANTS.rest_url[self._domain]),
                params=body_params,
                is_auth_required=True
            )
            return order_id
        except asyncio.CancelledError:
            raise
        except aiohttp.client.ServerDisconnectedError:
            self.logger().warning(f"{order_id} Server disconnect might be because of order restatement")
        except Exception as e:
            self.logger().network(
                f"Failed to cancel order {order_id}: {str(e)}." + (f"Params = {body_params}" if body_params else ""),
                exc_info=True,
                app_warning_msg=f"Failed to cancel the order {order_id} on Powertrade. "
                                f"Check API key and network connection."
            )

    async def _status_polling_loop(self):
        """
        Periodically update user balances and order status via REST API. This serves as a fallback measure for web
        socket API updates.
        """
        while True:
            try:
                self._poll_notifier = asyncio.Event()
                await self._poll_notifier.wait()
                await self._force_jwt_cache_update()
                await safe_wrapper(
                    self._update_balances(),
                )
                await safe_wrapper(self._update_order_status())
                if self._ws_adaptor and self._ws_adaptor.authenticated:
                    await self._request_trade_history()
                self._last_poll_timestamp = self.current_timestamp
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(str(e), exc_info=True)
                self.logger().network("Unexpected error while fetching account updates.",
                                      exc_info=True,
                                      app_warning_msg="Could not fetch account updates from Powertrade. "
                                                      "Check API key and network connection.")
                await asyncio.sleep(0.5)

    async def _update_balances(self):
        """
        Calls REST API to update total and available balances.
        """
        # Copied scheme from https://www.notion.so/powertrade/API-Endpoints-for-Hummingbot-Connector-6ef640891f80498fa798e7baa4f6af98
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()
        balance_info = await self._api_request(
            method="GET",
            path_url=CONSTANTS.BALANCE_URL(url=CONSTANTS.rest_url[self._domain]),
            is_auth_required=True
        )
        status = balance_info.get("balance_response", "response empty")
        if status == "response empty" or status == "error" or "error" in status:
            self.logger().error(f"Error update balances. {balance_info}")
            raise Exception(f"Error update balances. {balance_info}")

        deliverable_id_to_symbol_map = await self.get_deliverable_id_to_symbol_map()
        unmapped_deliverable_ids = set()
        for asset in balance_info["balance_response"]:
            asset_name = deliverable_id_to_symbol_map.get(asset["deliverable_id"])
            if asset_name is None:
                unmapped_deliverable_ids.add(asset['deliverable_id'])
                continue
            total_balance = Decimal(str(asset["cash_balance"]))
            available_balance = Decimal(str(asset["available_balance"]))
            if available_balance < Decimal("0.0"):
                if self.limit.get(asset_name):
                    available_balance = self.limit[asset_name] + available_balance
                    total_balance = self.limit[asset_name]
            self._account_available_balances[asset_name] = available_balance
            self._account_balances[asset_name] = total_balance
            remote_asset_names.add(asset_name)

        if len(unmapped_deliverable_ids) > 0:
            self.logger().info(f"Ids {unmapped_deliverable_ids} not currently present in deliverable_id to symbol map.")
            self.logger().info(f"Symbol map = {self.deliverable_id_to_symbol_map}")
            return

        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    async def _request_trade_history(self):
        # Method looks in the exchange history to check for any missing trade in local history.
        # If found, it will trigger an order_filled event to record it in local DB.
        # The minimum poll interval for order status is 3600 seconds.
        if not self._ws_adaptor:
            self.logger().error(f"The websocket startup in progress")
            return
        last_tick = int(self._last_trade_history_request_timestamp / self.HISTORY_RECON_INTERVAL)
        current_tick = int(self.current_timestamp / self.HISTORY_RECON_INTERVAL)

        if current_tick > last_tick or self._last_trade_history_request_timestamp == 0:
            trading_pairs = self._order_book_tracker._trading_pairs
            self.logger().info(f"Requesting trade history of {len(trading_pairs)} trading pairs.")
            try:
                if self._last_poll_timestamp == 0:
                    start_time_ts = int(pd.Timestamp.utcnow().timestamp() * 1e6 - self.DAY_IN_MICROSECONDS)
                else:
                    start_time_ts = int((pd.Timestamp.utcnow().timestamp() - self.HISTORY_RECON_INTERVAL) * 1e6)
                payload = {"trade_history_request": {"trade_time": str(start_time_ts),
                                                     "user_tag": f"history_{start_time_ts}"}}
                if not self._ws_adaptor:
                    self.logger().error(f"The websocket startup in progress")
                    return
                await self._ws_adaptor.send_request(payload)
                self.logger().info(f"Successfully to subscribe to trade channel {ujson.dumps(payload)}")
                self._last_trade_history_request_timestamp = self.current_timestamp
            except Exception as ex:
                self.logger().network(
                    f"Failed to fetch trade updates for {trading_pairs}. {str(ex)}",
                    app_warning_msg=f"Failed to fetch trade updates for {trading_pairs}. {str(ex)}"
                )

    async def _update_order_status(self):
        """
        Calls REST API to get status update for each in-flight order.
        """

        last_tick = int(self._last_poll_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL)
        current_tick = int(self.current_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL)

        if current_tick > last_tick and len(self._in_flight_orders) > 0:
            tracked_orders = list(self._in_flight_orders.values())

            tasks = []
            for tracked_order in tracked_orders:
                # ex_order_id = await tracked_order.get_exchange_order_id()

                query_params = {
                    "client_order_id": tracked_order.client_order_id,
                    "timestamp": str(int(self.current_timestamp)),
                }

                tasks.append(self._api_request(method="GET",
                                               path_url=CONSTANTS.QUERY_ORDER_URL(url=CONSTANTS.rest_url[self._domain]),
                                               params=query_params,
                                               is_auth_required=True)
                             )
            self.logger().debug(f"Polling for order status updates of {len(tasks)} orders.")
            await self._force_jwt_cache_update()
            order_results: List[Dict[str, Any]] = await safe_gather(*tasks, return_exceptions=True)

            for tracked_order, order_update in zip(tracked_orders, order_results):
                if isinstance(order_update, Exception):
                    self.logger().info(f"having exception during get order status {str(order_update)}")
                    continue
                if "query_order_response" not in order_update:
                    self.logger().info(f"Unexpected response from GET /order. "
                                       f"'query_order_response' field not in resp: {order_update}")
                    continue
                if "error" in order_update['query_order_response']:
                    if "exchange order id or client order id not found in order cache" in \
                            order_update['query_order_response'].get('reason'):
                        self.logger().info(f"Tracked order {tracked_order.client_order_id} does not exist in client.")
                        tracked_order.last_state = "not_found"
                        order_age = int(int(pd.Timestamp.utcnow().timestamp())) - int(
                            tracked_order.client_order_id[-16:]) / 1e6
                        if order_age >= 300:
                            self.trigger_event(MarketEvent.OrderCancelled,
                                               OrderCancelledEvent(
                                                   self.current_timestamp,
                                                   tracked_order.client_order_id))
                            self.stop_tracking_order(tracked_order.client_order_id)
                            continue
                        self.logger().info(f"Error in order {tracked_order.client_order_id} : {order_update['query_order_response'].get('reason')}")

                self._process_order_message(order_update["query_order_response"])

    def _process_order_message(self, order_msg: Dict[str, Any]):
        """
        Updates in-flight order and triggers trade, cancellation or failure event if needed.
        :param order_msg: The order response from either REST or web socket API (they are of the same format)
        """
        if "error" in order_msg:
            self.logger().info(f"Unexpected response from GET /order: {order_msg.get('reason')}")
            return
        client_order_id = order_msg["client_order_id"]
        if client_order_id not in self._in_flight_orders:
            return
        tracked_order = self._in_flight_orders[client_order_id]
        cancel_state = order_msg.get("cancel_state")

        if order_msg["order_state"] in {"partial_fill", "filled"}:
            for execution in order_msg.get("executions", []):
                if execution["trade_id"] not in tracked_order.trade_id_set:
                    tracked_order.executed_amount_base += Decimal(execution["executed_quantity"])
                    tracked_order.executed_amount_quote += Decimal(execution["executed_price"]) * \
                        Decimal(execution["executed_quantity"])
                    order_type = OrderType.LIMIT_MAKER if execution["liquidity_flag"] == 'maker' else OrderType.MARKET
                    trade_fee = self.get_fee(base_currency=tracked_order.base_asset,
                                             quote_currency=tracked_order.quote_asset,
                                             order_type=order_type,
                                             order_side=tracked_order.trade_type,
                                             amount=Decimal(execution["executed_quantity"]),
                                             price=Decimal(execution["executed_price"]))
                    tracked_order.fee_asset = tracked_order.quote_asset
                    tracked_order.fee_paid += trade_fee.fee_amount_in_quote(tracked_order.trading_pair,
                                                                            Decimal(execution["executed_price"]),
                                                                            Decimal(execution["executed_quantity"]))
                    tracked_order.trade_id_set.add(execution["trade_id"])
                    self.trigger_event(
                        MarketEvent.OrderFilled,
                        OrderFilledEvent(
                            self.current_timestamp,
                            client_order_id,
                            tracked_order.trading_pair,
                            tracked_order.trade_type,
                            order_type,
                            Decimal(execution["executed_price"]),
                            Decimal(execution["executed_quantity"]),
                            trade_fee,
                            execution["trade_id"]
                        )
                    )

            if order_msg["order_state"] == "filled":
                tracked_order.last_state = "filled"
                self.logger().info(f"The {tracked_order.trade_type.name} order "
                                   f"{tracked_order.client_order_id} has completed "
                                   f"according to order status REST API.")
                event_tag = MarketEvent.BuyOrderCompleted if tracked_order.trade_type is TradeType.BUY \
                    else MarketEvent.SellOrderCompleted
                event_class = BuyOrderCompletedEvent if tracked_order.trade_type is TradeType.BUY \
                    else SellOrderCompletedEvent
                self.trigger_event(event_tag,
                                   event_class(self.current_timestamp,
                                               tracked_order.client_order_id,
                                               tracked_order.base_asset,
                                               tracked_order.quote_asset,
                                               tracked_order.fee_asset,
                                               tracked_order.executed_amount_base,
                                               tracked_order.executed_amount_quote,
                                               tracked_order.fee_paid,
                                               tracked_order.order_type,
                                               tracked_order.exchange_order_id))
                self.stop_tracking_order(tracked_order.client_order_id)

        if order_msg['order_state'] == 'accepted' and cancel_state != "cancelled":
            self.logger().info(f"updating new order message {client_order_id}")
            self.logger().info(order_msg)
            self.logger().info("-----------------------------------------")
            tracked_order.update_exchange_order_id(order_msg['order_id'])

        if tracked_order.is_cancelled or (order_msg["cancel_state"] == "cancelled" and order_msg["order_state"] != 'partial_fill'):
            tracked_order.last_state = "cancelled"
            self.logger().info(f"Successfully cancelled order {client_order_id}.")
            self.logger().info(f"{order_msg}")
            self.trigger_event(MarketEvent.OrderCancelled,
                               OrderCancelledEvent(
                                   self.current_timestamp,
                                   client_order_id))
            tracked_order.cancelled_event.set()
            self.stop_tracking_order(client_order_id)

        elif tracked_order.is_failure or order_msg["order_state"] == "rejected":
            tracked_order.last_state = "rejected"
            self.logger().info(f"The market order {client_order_id} has failed according to order status API. "
                               f"Order Message: {order_msg}")
            self.trigger_event(MarketEvent.OrderFailure,
                               MarketOrderFailureEvent(
                                   self.current_timestamp,
                                   client_order_id,
                                   tracked_order.order_type
                               ))
            self.stop_tracking_order(client_order_id)



    def _process_trade_message(self, trade_msg: Dict[str, Any]):
        self.logger().info(f"starting process trade message")
        client_order_id = trade_msg["client_order_id"]
        if client_order_id not in self._in_flight_orders:
            return
        tracked_order = self._in_flight_orders[client_order_id]
        if trade_msg["trade_id"] not in tracked_order.trade_id_set:
            tracked_order.executed_amount_base += Decimal(trade_msg["executed_quantity"])
            tracked_order.executed_amount_quote += Decimal(trade_msg["executed_price"]) * \
                Decimal(trade_msg["executed_quantity"])
            order_type = OrderType.LIMIT_MAKER if trade_msg["liquidity_flag"] == 'added' else OrderType.MARKET
            trade_fee = self.get_fee(base_currency=tracked_order.base_asset,
                                     quote_currency=tracked_order.quote_asset,
                                     order_type=order_type,
                                     order_side=tracked_order.trade_type,
                                     amount=Decimal(trade_msg["executed_quantity"]),
                                     price=Decimal(trade_msg["executed_price"]))
            tracked_order.fee_asset = tracked_order.quote_asset
            tracked_order.fee_paid += trade_fee.fee_amount_in_quote(tracked_order.trading_pair,
                                                                    Decimal(trade_msg["executed_price"]),
                                                                    Decimal(trade_msg["executed_quantity"]))

            tracked_order.trade_id_set.add(trade_msg["trade_id"])
            self.trigger_event(
                MarketEvent.OrderFilled,
                OrderFilledEvent(
                    self.current_timestamp,
                    client_order_id,
                    tracked_order.trading_pair,
                    tracked_order.trade_type,
                    tracked_order.order_type,
                    Decimal(trade_msg["executed_price"]),
                    Decimal(trade_msg["executed_quantity"]),
                    trade_fee,
                    trade_msg["trade_id"]
                )
            )

        if (math.isclose(tracked_order.executed_amount_base, tracked_order.amount) or
                tracked_order.executed_amount_base >= tracked_order.amount):
            tracked_order.last_state = "filled"
            self.logger().info(f"The {tracked_order.trade_type.name} order "
                               f"{tracked_order.client_order_id} has completed "
                               f"according to websocket trade updates.")
            event_tag = (MarketEvent.BuyOrderCompleted if tracked_order.trade_type is TradeType.BUY
                         else MarketEvent.SellOrderCompleted)
            event_class = (BuyOrderCompletedEvent if tracked_order.trade_type is TradeType.BUY
                           else SellOrderCompletedEvent)
            self.trigger_event(event_tag,
                               event_class(self.current_timestamp,
                                           tracked_order.client_order_id,
                                           tracked_order.base_asset,
                                           tracked_order.quote_asset,
                                           tracked_order.fee_asset,
                                           tracked_order.executed_amount_base,
                                           tracked_order.executed_amount_quote,
                                           tracked_order.fee_paid,
                                           tracked_order.order_type,
                                           tracked_order.exchange_order_id))
            self.stop_tracking_order(tracked_order.client_order_id)

    def _process_order_cancellation_message(self, cancellation_msg: Dict[str, Any]):
        client_order_id = cancellation_msg["client_order_id"]
        if client_order_id not in self._in_flight_orders:
            return
        tracked_order = self._in_flight_orders[client_order_id]
        if tracked_order.trading_pair != powertrade_utils.convert_from_exchange_trading_pair(
                cancellation_msg["symbol"]):
            self.logger().info("Warning: Cancelled order symbol does not match with tracked order trading_pair."
                               f"Failed to cancel order {client_order_id}")
        # This message does not provide us more info about this is filled or not.
        # we will handle cancel in REST API, just keep this client id alive.
        tracked_order.last_state = "cancelled"
        self.logger().info(f"Successfully cancelled order {client_order_id} as per User Stream. {json.dumps(cancellation_msg)}")
        self.trigger_event(MarketEvent.OrderCancelled,
                           OrderCancelledEvent(
                               self.current_timestamp,
                               client_order_id))
        tracked_order.cancelled_event.set()
        #self.stop_tracking_order(client_order_id)

    def _process_trade_history_message(self, trade_history_msg: Dict[str, Any]):
        for trade_msg in trade_history_msg['executions']:
            symbol = self.tradeable_entity_id_to_symbol_map.get(trade_msg['tradeable_entity_id'])
            base_asset, quote_asset = powertrade_utils.split_trading_pair(symbol)
            trading_pair = powertrade_utils.convert_from_exchange_trading_pair(symbol)
            timestamp = trade_msg["utc_timestamp"]
            executed_price = Decimal(trade_msg["executed_price"])
            executed_quantity = Decimal(trade_msg["executed_quantity"])
            side = trade_msg['side']
            trade_type = TradeType.BUY if side == 'buy' else TradeType.SELL
            order_id = str(trade_msg["order_id"])
            trade_id = trade_msg["trade_id"]
            client_order_id = str(trade_msg["client_order_id"])

            if self.is_confirmed_new_order_filled_event(trade_id, client_order_id, trading_pair):
                # Should check if this is a partial filling of a in_flight order.
                # In that case, user_stream or _update_order_fills_from_trades will take care when fully filled.
                if not any(trade_id in in_flight_order.trade_id_set for in_flight_order in self._in_flight_orders.values()):
                    trade_fee = self.get_fee(base_currency=base_asset,
                                             quote_currency=quote_asset,
                                             order_type=OrderType.LIMIT_MAKER,  # Information missing in msg
                                             order_side=trade_type,
                                             amount=executed_quantity,
                                             price=executed_price)
                    self.trigger_event(MarketEvent.OrderFilled,
                                         OrderFilledEvent(
                                             timestamp,
                                             client_order_id,
                                             trading_pair,
                                             trade_type,
                                             OrderType.LIMIT_MAKER,
                                             executed_price,
                                             executed_quantity,
                                             trade_fee,
                                             exchange_trade_id=trade_id
                                         ))
                    self.logger().info(f"Recreating missing trade in TradeFill: {trade_msg}")

    async def get_open_orders(self) -> List[OpenOrder]:
        ret_val = []
        result = await self._api_request(
            method="GET",
            path_url=CONSTANTS.OPEN_ORDER_URL(url=CONSTANTS.rest_url[self._domain]),
            is_auth_required=True
        )
        response = result.get("query_open_orders_response", {})
        if "open_orders" not in response:
            self.logger().info(f"Unexpected response from GET {CONSTANTS.OPEN_ORDER_URL(url=CONSTANTS.rest_url[self._domain])}. "
                               f"Response: {result} ")
        timestamp = int(response['utc_timestamp'])
        if not isinstance(self._symbol_to_tradeable_entity_id_map, dict):
            # In case open orders is called before market trading rules loop is called, need to force it to map pairs
            try:
                await asyncio.wait_for(self._update_trading_rules(), timeout=2.0)
            except asyncio.TimeoutError:
                self.logger().info("FAILED W TIMEOUT")
                pass
        for order in response['open_orders']:
            executions = order["executions"]
            executed_amount = sum(Decimal(execution["executed_quantity"]) for execution in executions) \
                if executions else Decimal("0")
            ret_val.append(
                OpenOrder(
                    client_order_id=order["client_order_id"],
                    trading_pair=powertrade_utils.convert_from_exchange_trading_pair(
                        self.tradeable_entity_id_to_symbol_map.get(order["tradeable_entity_id"],
                                                                   order["tradeable_entity_id"])),
                    price=Decimal(str(order["price"])),
                    amount=Decimal(str(order["quantity"])),
                    executed_amount=executed_amount,
                    status=order["order_state"],
                    order_type=OrderType.LIMIT,
                    is_buy=True if order["side"].lower() == "buy" else False,
                    time=timestamp,
                    exchange_order_id=order["order_id"]
                )
            )
        return ret_val

    async def cancel_all(self, timeout_seconds: float):
        """
        Cancels all in-flight orders and waits for cancellation results.
        Used by bot's top level stop and exit commands (cancelling outstanding orders on exit)
        :param timeout_seconds: The timeout at which the operation will be canceled.
        :returns List of CancellationResult which indicates whether each order is successfully cancelled.
        """
        if self._trading_pairs is None:
            raise Exception("cancel_all can only be used when trading_pairs are specified.")
        cancellation_results = []
        self.logger().info(f"starting process cancel all")
        try:
            tasks = []
            for tracked_order in self.in_flight_orders.values():
                body_params = {
                    "order_id": tracked_order.exchange_order_id,
                    "timestamp": str(int(self.current_timestamp)) if not math.isnan(self.current_timestamp) else "0"
                }
                self.logger().info(f"track body params {ujson.dumps(body_params)}")
                tasks.append(self._api_request(
                    method="DELETE",
                    path_url=CONSTANTS.CANCEL_ORDER_URL(url=CONSTANTS.rest_url[self._domain]),
                    params=body_params,
                    is_auth_required=True
                ))

            await self._force_jwt_cache_update()
            await safe_gather(*tasks, return_exceptions=True)

            open_orders = await self.get_open_orders()
            for cl_order_id, tracked_order in self._in_flight_orders.items():
                open_order = [o for o in open_orders if o.client_order_id == cl_order_id]
                if not open_order:
                    cancellation_results.append(CancellationResult(cl_order_id, True))
                    self.trigger_event(MarketEvent.OrderCancelled,
                                       OrderCancelledEvent(self.current_timestamp, cl_order_id))
                    tracked_order.last_state = 'cancelled'
                else:
                    cancellation_results.append(CancellationResult(cl_order_id, False))
        except Exception:
            self.logger().network(
                "Failed to cancel all orders.",
                exc_info=True,
                app_warning_msg="Failed to cancel all orders on Powertrade. Check API key and network connection."
            )
        return cancellation_results

    def tick(self, timestamp: float):
        """
        Is called automatically by the clock for each clock's tick (1 second by default).
        It checks if status polling task is due for execution.
        """
        now = int(pd.Timestamp.utcnow().timestamp())
        poll_interval = (self.SHORT_POLL_INTERVAL
                         if now - self._user_stream_tracker.last_recv_time > 60.0
                         else self.LONG_POLL_INTERVAL)
        last_tick = int(self._last_timestamp / poll_interval)
        current_tick = int(timestamp / poll_interval)
        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()
        self._last_timestamp = timestamp

    def get_fee(self,
                base_currency: str,
                quote_currency: str,
                order_type: OrderType,
                order_side: TradeType,
                amount: Decimal,
                price: Decimal = s_decimal_NaN) -> TradeFee:
        """
        To get trading fee, this function is simplified by using fee override configuration. Most parameters to this
        function are ignore except order_type. Use OrderType.LIMIT_MAKER to specify you want trading fee for
        maker order.
        """
        is_maker = order_type is OrderType.LIMIT_MAKER
        return TradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _iter_user_event_queue(self) -> AsyncIterable[Dict[str, any]]:
        while True:
            try:
                yield await self._user_stream_tracker.user_stream.get()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unknown error. Retrying after 1 seconds.",
                    exc_info=True,
                    app_warning_msg="Could not fetch user events from Powertrade. Check API key and network connection."
                )
                await asyncio.sleep(1.0)

    async def _user_stream_event_listener(self):
        """
        Listens to message in _user_stream_tracker.user_stream queue. The messages are put in by
        PowertradeAPIUserStreamDataSource.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                if "channel" not in event_message or event_message["channel"] not in CONSTANTS.WS_PRIVATE_CHANNELS:
                    continue
                channel = event_message["channel"]

                if channel == "balance":
                    for asset, balance_details in event_message["data"].items():
                        self._account_balances[asset] = Decimal(str(balance_details["total"]))
                        self._account_available_balances[asset] = Decimal(str(balance_details["available"]))
                elif channel == "trade":
                    self._process_trade_message(event_message)
                elif channel == "cancel_order":
                    self._process_order_cancellation_message(event_message)
                elif channel == 'trade_history':
                    self._process_trade_history_message(event_message)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await asyncio.sleep(5.0)

    async def _force_jwt_cache_update(self):
        # This ensures that cache update does not happen during quasi-parallel tasks
        try:
            if self._powertrade_auth.credential_secret_has_expired:
                await self._api_request(
                    method="GET",
                    path_url=CONSTANTS.BALANCE_URL(url=CONSTANTS.rest_url[self._domain]),
                    is_auth_required=True
                )
                self.logger().info("Forced jwt cache update")
        except Exception as e:
            self.logger().error("Failed in _force_jwt_cache_update")
            self.logger().exception(e)
