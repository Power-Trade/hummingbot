#!/usr/bin/env python
import aiohttp
import asyncio
import logging
import pandas as pd

import hummingbot.connector.exchange.powertrade.powertrade_constants as CONSTANTS

from typing import (
    Any,
    Dict,
    List,
    Optional,
)
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.powertrade.powertrade_utils import (
    convert_to_exchange_trading_pair,
    convert_from_exchange_trading_pair,
    convert_snapshot_message_to_order_book_row,
)
from hummingbot.connector.exchange.powertrade.powertrade_websocket_adaptor import PowertradeWebSocketAdaptor
from hummingbot.connector.exchange.powertrade.powertrade_order_book import PowertradeOrderBook
from hummingbot.connector.exchange.powertrade.powertrade_subscription_tracker import PowerTradeSubscriptionTracker


class PowertradeAPIOrderBookDataSource(OrderBookTrackerDataSource):
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, trading_pairs: List[str] = None,
                 subscription_tracker: PowerTradeSubscriptionTracker = None,
                 domain: str = "prod"):
        super().__init__(trading_pairs)
        self._domain = domain
        self._trading_pairs: List[str] = trading_pairs
        self._snapshot_msg: Dict[str, any] = {}
        self._last_recv_time: float = 0
        self._subscription_tracker: PowerTradeSubscriptionTracker = subscription_tracker

    @classmethod
    async def get_last_traded_prices(cls, trading_pairs: List[str], domain: str = "prod") -> Dict[str, float]:
        result = {}
        async with aiohttp.ClientSession() as client:
            async with client.get(f"{CONSTANTS.SUMMARY_24HS_URL(url=CONSTANTS.pts_url[domain])}") as response:
                if response.status == 200:
                    resp_json = await response.json()
                    for market in resp_json:
                        if market["symbol"] in trading_pairs:
                            try:
                                result[market["symbol"]] = float(market["last_price"])
                            except (KeyError, TypeError, ValueError):
                                # In case last_price is missing (red herring), use mid-price
                                try:
                                    result[market["symbol"]] = (float(market["best_bid"]) +
                                                                float(market["best_ask"])) / 2
                                except Exception:
                                    # In case there's some value missing, will omit symbol
                                    pass

        return result

    @staticmethod
    async def fetch_trading_pairs(domain: str = "prod") -> List[str]:
        try:
            async with aiohttp.ClientSession() as client:
                async with client.get(f"{CONSTANTS.ENTITIES_AND_RULES_URL(url=CONSTANTS.rest_url[domain])}") as response:
                    trading_pairs = []
                    if response.status == 200:
                        resp_json: Dict[str, Any] = await response.json()
                        for market in resp_json["entities_and_rules_response"]["symbols"]:
                            if market["status"] == "active":
                                trading_pair = convert_from_exchange_trading_pair(market["symbol"],
                                                                                  market["quote_asset"])

                                if trading_pair:
                                    trading_pairs.append(trading_pair)

                    return trading_pairs
        except Exception:
            # Do nothing if the request fails lo-- there will be no autocomplete for trading pairs
            pass

    @staticmethod
    async def get_order_book_data(trading_pair: str, domain: str = "prod") -> Dict[str, any]:
        """
        Get whole orderbook
        """
        async with aiohttp.ClientSession() as client:
            async with client.get(url=f"{CONSTANTS.ORDER_BOOK_URL(url=CONSTANTS.rest_url[domain])}",
                                  params={"symbol": convert_to_exchange_trading_pair(trading_pair),
                                          "depth": 100}) as response:
                if response.status != 200:
                    raise IOError(
                        f"Error fetching OrderBook for {trading_pair} at {CONSTANTS.ORDER_BOOK_URL(url=CONSTANTS.rest_url[domain])}. "
                        f"HTTP {response.status}. Response: {await response.json()}"
                    )
                msg = await response.json()
                if "orderbook_response" in msg:
                    return msg["orderbook_response"]
                else:
                    raise Exception(f"Failed to retrieve orderbook_response for {trading_pair}.")

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        snapshot: Dict[str, Any] = await self.get_order_book_data(trading_pair)
        snapshot_timestamp: int = int(pd.Timestamp.utcnow().timestamp() * 1e3)
        snapshot_msg: OrderBookMessage = PowertradeOrderBook.snapshot_message_from_exchange(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        order_book = self.order_book_create_function()
        bids, asks = convert_snapshot_message_to_order_book_row(snapshot_msg)
        order_book.apply_snapshot(bids, asks, snapshot_msg.update_id)
        return order_book

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        Listen for trades using websocket public trades channel
        """
        # Not implemented in market-proxy. Not a priority
        return

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        Listen for orderbook snapshot+diffs using websocket book channel
        """
        # This is not applicable anymore since websocket adaptor dispatches messages
        return

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        Get orderbook snapshots by fetching REST endpoint orderbook
        """
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    try:
                        snapshot: Dict[str, any] = await self.get_order_book_data(trading_pair)
                        snapshot_timestamp: int = int(pd.Timestamp.utcnow().timestamp() * 1e3)
                        snapshot_msg: OrderBookMessage = PowertradeOrderBook.snapshot_message_from_exchange(
                            msg=snapshot,
                            timestamp=snapshot_timestamp,
                        )
                        output.put_nowait(snapshot_msg)
                        self.logger().debug(f"Saved order book snapshot for {trading_pair}")
                        # Be careful not to go above API rate limits.
                        await asyncio.sleep(5.0)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        self.logger().network(
                            "Unexpected error with WebSocket connection.",
                            exc_info=True,
                            app_warning_msg="Unexpected error with WebSocket connection. Retrying in 5 seconds. "
                                            "Check network connection."
                        )
                        await asyncio.sleep(5.0)
                this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
                next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
                delta: float = next_hour.timestamp() - pd.Timestamp.utcnow().timestamp()
                await asyncio.sleep(delta)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await asyncio.sleep(5.0)

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    def update_last_recv_time(self):
        self._last_recv_time = int(pd.Timestamp.utcnow().timestamp())

    @property
    def subscription_tracker(self):
        return self._subscription_tracker

    async def subscribe_to_channels(self, ws_adaptor: PowertradeWebSocketAdaptor):
        for trading_pair in self._trading_pairs:
            user_tag = f"{trading_pair}_ob_{str(int(pd.Timestamp.utcnow().timestamp()*1e3))}"
            self.subscription_tracker.subscription_user_tags[trading_pair] = user_tag
            params: Dict[str, Any] = {
                "subscribe": {
                    "market_id": "0",
                    "symbol": convert_to_exchange_trading_pair(trading_pair),
                    "type": "snap_with_deltas",
                    "interval": "100",
                    "user_tag": user_tag,
                }
            }
            await ws_adaptor.send_request(params)
        return

    def process_order_book_diff(self, msg: Dict[str, Any], output: asyncio.Queue):
        msg_timestamp: int = int(pd.Timestamp.utcnow().timestamp() * 1e3)
        if "snapshot" in msg:
            # First response from websocket is a snapshot. This is only when reset = True
            snapshot_msg: OrderBookMessage = PowertradeOrderBook.snapshot_message_from_exchange(
                msg=msg["snapshot"],
                timestamp=msg_timestamp,
            )
            output.put_nowait(snapshot_msg)
        elif "deltas" in msg:
            diff_msg: OrderBookMessage = PowertradeOrderBook.diff_message_from_exchange(
                msg=msg["deltas"],
                timestamp=msg_timestamp,
            )
            output.put_nowait(diff_msg)
