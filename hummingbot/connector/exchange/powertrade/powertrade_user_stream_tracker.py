#!/usr/bin/env python

import asyncio
import logging

import hummingbot.connector.exchange.powertrade.powertrade_constants as CONSTANTS

from typing import (
    Optional,
    List,
    Dict,
    Any
)

from hummingbot.connector.exchange.powertrade.powertrade_subscription_tracker import PowerTradeSubscriptionTracker
from hummingbot.connector.exchange.powertrade.powertrade_api_user_stream_data_source import \
    PowertradeAPIUserStreamDataSource
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.data_type.user_stream_tracker import UserStreamTracker
from hummingbot.logger import HummingbotLogger


class PowertradeUserStreamTracker(UserStreamTracker):
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self,
                 trading_pairs: Optional[List[str]] = [],
                 domain: str = "prod"):
        super().__init__()
        self._domain: str = domain
        self._trading_pairs: List[str] = trading_pairs
        self._ev_loop: asyncio.events.AbstractEventLoop = asyncio.get_event_loop()
        self._data_source: Optional[UserStreamTrackerDataSource] = None
        self._user_stream_tracking_task: Optional[asyncio.Task] = None

    @property
    def data_source(self) -> UserStreamTrackerDataSource:
        """
        *required
        Initializes a user stream data source (user specific order diffs from live socket stream)
        :return: OrderBookTrackerDataSource
        """
        if not self._data_source:
            self._data_source = PowertradeAPIUserStreamDataSource(
                trading_pairs=self._trading_pairs,
                subscription_tracker=PowerTradeSubscriptionTracker(self._trading_pairs),
                domain=self._domain
            )
        return self._data_source

    @property
    def exchange_name(self) -> str:
        """
        *required
        Name of the current exchange
        """
        if self._domain == "prod":
            return CONSTANTS.EXCHANGE_NAME
        else:
            return f"{CONSTANTS.EXCHANGE_NAME}_{self._domain}"

    async def start(self):
        return

    @property
    def ready(self):
        return self.data_source.subscription_tracker.ready

    # Websocket adaptor logic.

    def is_eligible_for_message(self, message: Dict[str, Any]):
        return any(f(message) for f in (self.is_trade_update_msg, self.is_order_cancellation_msg, self.is_trade_history_msg))

    @classmethod
    def is_trade_update_msg(cls, message: Dict[str, Any]):
        return 'execution' in message

    @classmethod
    def is_order_cancellation_msg(cls, message: Dict[str, Any]):
        return 'cancel_order' in message

    @classmethod
    def is_trade_history_msg(cls, message: Dict[str, Any]):
        return 'trade_history_response' in message

    def process_message(self, message: Dict[str, Any]):
        self.data_source.update_last_recv_time()
        if self.is_trade_update_msg(message):
            self._data_source.process_trade_update(message, self.user_stream)
        elif self.is_order_cancellation_msg(message):
            self._data_source.process_order_cancellation(message, self.user_stream)
        elif self.is_trade_history_msg(message):
            self._data_source.process_trade_history(message, self.user_stream)

    def process_control_message(self, message):
        return self.data_source.subscription_tracker.process_control_message(message)

    # End of websocket adaptor logic
