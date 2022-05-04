#!/usr/bin/env python

import asyncio
import logging
import pandas as pd
from hummingbot.connector.exchange.powertrade.powertrade_websocket_adaptor import PowertradeWebSocketAdaptor
from hummingbot.connector.exchange.powertrade.powertrade_subscription_tracker import PowerTradeSubscriptionTracker
from hummingbot.connector.exchange.powertrade.powertrade_utils import (
    convert_to_exchange_trading_pair,
)

import hummingbot.connector.exchange.powertrade.powertrade_constants as CONSTANTS

from typing import (
    Any,
    Dict,
    List,
    Optional,
)

from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger


class PowertradeAPIUserStreamDataSource(UserStreamTrackerDataSource):
    MAX_RETRIES = 20
    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self,
                 trading_pairs: Optional[List[str]] = [],
                 subscription_tracker: PowerTradeSubscriptionTracker = None,
                 domain: str = "prod"):
        self._domain: str = domain
        self._trading_pairs = trading_pairs
        self._subscription_tracker: PowerTradeSubscriptionTracker = subscription_tracker
        self._last_recv_time: float = 0
        super().__init__()

    @property
    def exchange_name(self) -> str:
        if self._domain == "prod":
            return CONSTANTS.EXCHANGE_NAME
        else:
            return f"{CONSTANTS.EXCHANGE_NAME}_{self._domain}"

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    def update_last_recv_time(self):
        self._last_recv_time = int(pd.Timestamp.utcnow().timestamp())

    async def listen_for_user_stream(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        return

    @property
    def subscription_tracker(self):
        return self._subscription_tracker

    async def subscribe_to_channels(self, ws_adaptor: PowertradeWebSocketAdaptor):
        for trading_pair in self._trading_pairs:
            user_tag = f"{trading_pair}_us_{int(pd.Timestamp.utcnow().timestamp() * 1e3)}"
            self.subscription_tracker.subscription_user_tags[trading_pair] = user_tag
            params: Dict[str, Any] = {
                "subscribe": {
                    "market_id": "0",
                    "symbol": convert_to_exchange_trading_pair(trading_pair),
                    "type": "trades",
                    "user_tag": user_tag,
                }
            }
            await ws_adaptor.send_request(params)
            self.update_last_recv_time()
        return

    def process_trade_update(self, msg: Dict[str, Any], output: asyncio.Queue):
        trade_msg: Dict[str, Any] = msg['execution']
        trade_msg["channel"] = "trade"
        output.put_nowait(trade_msg)

    def process_order_cancellation(self, msg: Dict[str, Any], output: asyncio.Queue):
        cancellation_msg: Dict[str, Any] = msg['cancel_order']
        cancellation_msg["channel"] = "cancel_order"
        output.put_nowait(cancellation_msg)

    def process_trade_history(self, msg: Dict[str, Any], output: asyncio.Queue):
        trade_history_msg: Dict[str, Any] = msg['trade_history_response']
        trade_history_msg["channel"] = "trade_history"
        output.put_nowait(trade_history_msg)
