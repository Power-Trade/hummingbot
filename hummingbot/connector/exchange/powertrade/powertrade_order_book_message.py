#!/usr/bin/env python

from typing import (
    Dict,
    List,
    Optional,
)

from hummingbot.connector.exchange.powertrade.powertrade_utils import convert_from_exchange_trading_pair
from hummingbot.core.data_type.order_book_row import OrderBookRow
from hummingbot.core.data_type.order_book_message import (
    OrderBookMessage,
    OrderBookMessageType,
)


class PowertradeOrderBookMessage(OrderBookMessage):
    def __new__(
        cls,
        message_type: OrderBookMessageType,
        content: Dict[str, any],
        timestamp: Optional[float] = None,
        *args,
        **kwargs,
    ):
        if timestamp is None:
            if message_type is OrderBookMessageType.SNAPSHOT:
                raise ValueError("timestamp must not be None when initializing snapshot messages.")
            timestamp = content["timestamp"]

        return super(PowertradeOrderBookMessage, cls).__new__(
            cls, message_type, content, timestamp=timestamp, *args, **kwargs
        )

    @property
    def update_id(self) -> int:
        if self.type in [OrderBookMessageType.DIFF, OrderBookMessageType.SNAPSHOT]:
            return int(self.timestamp)
        else:
            return -1

    @property
    def trade_id(self) -> int:
        if self.type is OrderBookMessageType.TRADE:
            return int(self.content.get("trade_id"))
        return -1

    @property
    def trading_pair(self) -> str:
        if "symbol" in self.content:
            return convert_from_exchange_trading_pair(self.content["symbol"])
        else:
            raise ValueError("symbol not found in message content")

    @property
    def asks(self) -> List[OrderBookRow]:
        entries = []
        entries = self.content["sell"]

        return [
            OrderBookRow(float(entry["price"]), float(entry["quantity"]), self.update_id) for entry in entries
        ]

    @property
    def bids(self) -> List[OrderBookRow]:
        entries = []
        entries = self.content["buy"]

        return [
            OrderBookRow(float(entry["price"]), float(entry["quantity"]), self.update_id) for entry in entries
        ]

    def __eq__(self, other) -> bool:
        return self.type == other.type and self.timestamp == other.timestamp

    def __lt__(self, other) -> bool:
        if self.timestamp != other.timestamp:
            return self.timestamp < other.timestamp
        else:
            """
            If timestamp is the same, the ordering is snapshot < diff < trade
            """
            return self.type.value < other.type.value
