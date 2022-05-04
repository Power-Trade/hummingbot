#!/usr/bin/env python
import asyncio
import bisect
import logging
import hummingbot.connector.exchange.powertrade.powertrade_constants as CONSTANTS
import pandas as pd

from collections import defaultdict, deque
from typing import Optional, Dict, List, Deque, Any
from hummingbot.core.data_type.order_book_message import OrderBookMessageType
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker
from hummingbot.client.config.global_config_map import global_config_map
from hummingbot.connector.exchange.powertrade import powertrade_utils
from hummingbot.connector.exchange.powertrade.powertrade_auth import PowertradeAuth
from hummingbot.connector.exchange.powertrade.powertrade_subscription_tracker import PowerTradeSubscriptionTracker
from hummingbot.connector.exchange.powertrade.powertrade_order_book_message import PowertradeOrderBookMessage
from hummingbot.connector.exchange.powertrade.powertrade_api_order_book_data_source import PowertradeAPIOrderBookDataSource
from hummingbot.connector.exchange.powertrade.powertrade_order_book import PowertradeOrderBook
from hummingbot.logger import HummingbotLogger


class PowertradeOrderBookTracker(OrderBookTracker):
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, trading_pairs: Optional[List[str]] = None, domain: str = "prod"):
        super().__init__(PowertradeAPIOrderBookDataSource(trading_pairs,
                                                          PowerTradeSubscriptionTracker(trading_pairs),
                                                          domain),
                         trading_pairs)
        self._domain = domain
        self._ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        self._order_book_snapshot_stream: asyncio.Queue = asyncio.Queue()
        self._order_book_diff_stream: asyncio.Queue = asyncio.Queue()
        self._order_book_trade_stream: asyncio.Queue = asyncio.Queue()
        self._process_msg_deque_task: Optional[asyncio.Task] = None
        self._past_diffs_windows: Dict[str, Deque] = {}
        self._order_books: Dict[str, PowertradeOrderBook] = {}
        self._saved_message_queues: Dict[str, Deque[PowertradeOrderBookMessage]] = \
            defaultdict(lambda: deque(maxlen=1000))
        self._order_book_stream_listener_task: Optional[asyncio.Task] = None
        self._order_book_trade_listener_task: Optional[asyncio.Task] = None
        self._ws_adaptor_task = None

    def start(self):
        super(PowertradeOrderBookTracker, self).start()

    def stop(self):
        super(PowertradeOrderBookTracker, self).stop()

    @property
    def exchange_name(self) -> str:
        """
        Name of the current exchange
        """
        if self._domain == "prod":
            return CONSTANTS.EXCHANGE_NAME
        else:
            return f"{CONSTANTS.EXCHANGE_NAME}_{self._domain}"

    async def _track_single_book(self, trading_pair: str):
        """
        Update an order book with changes from the latest batch of received messages
        """
        past_diffs_window: Deque[PowertradeOrderBookMessage] = deque()
        self._past_diffs_windows[trading_pair] = past_diffs_window

        message_queue: asyncio.Queue = self._tracking_message_queues[trading_pair]
        order_book: PowertradeOrderBook = self._order_books[trading_pair]

        last_message_timestamp: float = pd.Timestamp.utcnow().timestamp()
        diff_messages_accepted: int = 0

        while True:
            try:
                message: PowertradeOrderBookMessage = None
                saved_messages: Deque[PowertradeOrderBookMessage] = self._saved_message_queues[trading_pair]
                # Process saved messages first if there are any
                if len(saved_messages) > 0:
                    message = saved_messages.popleft()
                else:
                    message = await message_queue.get()

                if message.type is OrderBookMessageType.DIFF:
                    bids, asks = powertrade_utils.convert_diff_message_to_order_book_row(message)
                    order_book.apply_diffs(bids, asks, message.update_id)
                    past_diffs_window.append(message)
                    while len(past_diffs_window) > self.PAST_DIFF_WINDOW_SIZE:
                        past_diffs_window.popleft()
                    diff_messages_accepted += 1

                    # Output some statistics periodically.
                    now: float = pd.Timestamp.utcnow().timestamp()
                    if int(now / 60.0) > int(last_message_timestamp / 60.0):
                        self.logger().debug(f"Processed {diff_messages_accepted} order book diffs for {trading_pair}.")
                        diff_messages_accepted = 0
                    last_message_timestamp = now
                elif message.type is OrderBookMessageType.SNAPSHOT:
                    past_diffs: List[PowertradeOrderBookMessage] = list(past_diffs_window)
                    # only replay diffs later than snapshot, first update active order with snapshot then replay diffs
                    replay_position = bisect.bisect_right(past_diffs, message)
                    replay_diffs = past_diffs[replay_position:]
                    s_bids, s_asks = powertrade_utils.convert_snapshot_message_to_order_book_row(message)
                    order_book.apply_snapshot(s_bids, s_asks, message.update_id)
                    for diff_message in replay_diffs:
                        d_bids, d_asks = powertrade_utils.convert_diff_message_to_order_book_row(diff_message)
                        order_book.apply_diffs(d_bids, d_asks, diff_message.update_id)

                    self.logger().debug(f"Processed order book snapshot for {trading_pair}.")
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    f"Unexpected error processing order book messages for {trading_pair}.",
                    exc_info=True,
                    app_warning_msg="Unexpected error processing order book messages. Retrying after 5 seconds."
                )
                await asyncio.sleep(5.0)

    @property
    def ready(self):
        return self.data_source.subscription_tracker.ready

    # Websocket adaptor logic.
    def is_eligible_for_message(self, message: Dict[str, Any]):
        return any(f(message) for f in (self.is_snapshot_or_delta_order_book_msg,))

    @classmethod
    def is_snapshot_or_delta_order_book_msg(cls, message: Dict[str, Any]):
        return any(['snapshot' in message, 'deltas' in message])

    def process_message(self, message: Dict[str, Any]):
        self.data_source.update_last_recv_time()
        if self.is_snapshot_or_delta_order_book_msg(message):
            self._data_source.process_order_book_diff(message, self._order_book_diff_stream)

    def process_control_message(self, message):
        return self.data_source.subscription_tracker.process_control_message(message)
    # End of websocket adaptor logic
