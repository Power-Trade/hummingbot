#!/usr/bin/env python

import asyncio

from decimal import Decimal
from typing import (
    Any,
    Dict,
    Optional,
)

from hummingbot.connector.in_flight_order_base import InFlightOrderBase
from hummingbot.core.event.events import (
    OrderType,
    TradeType
)

from async_timeout import timeout

class PowertradeInFlightOrder(InFlightOrderBase):
    def __init__(self,
                 client_order_id: str,
                 exchange_order_id: Optional[str],
                 trading_pair: str,
                 order_type: OrderType,
                 trade_type: TradeType,
                 price: Decimal,
                 amount: Decimal,
                 initial_state: str = "open"):
        super().__init__(
            client_order_id,
            exchange_order_id,
            trading_pair,
            order_type,
            trade_type,
            price,
            amount,
            initial_state,
        )
        self.trade_id_set = set()
        self.cancelled_event = asyncio.Event()



    @property
    def is_done(self) -> bool:
        return self.last_state in {"filled", "cancelled"}

    @property
    def is_failure(self) -> bool:
        return self.last_state in {"rejected"}

    @property
    def is_cancelled(self) -> bool:
        return self.last_state in {"cancelled"}

    async def get_exchange_order_id(self):
        if self.exchange_order_id is None:
            async with timeout(10):
                await self.exchange_order_id_update_event.wait()
        return self.exchange_order_id

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> InFlightOrderBase:
        """
        :param data: json data from API
        :return: formatted InFlightOrder
        """
        retval = PowertradeInFlightOrder(
            data["client_order_id"],
            data["exchange_order_id"],
            data["trading_pair"],
            getattr(OrderType, data["order_type"]),
            getattr(TradeType, data["trade_type"]),
            Decimal(data["price"]),
            Decimal(data["amount"]),
            data["last_state"]
        )
        retval.executed_amount_base = Decimal(data["executed_amount_base"])
        retval.executed_amount_quote = Decimal(data["executed_amount_quote"])
        retval.fee_asset = data["fee_asset"]
        retval.fee_paid = Decimal(data["fee_paid"])
        retval.last_state = data["last_state"]
        return retval

    def update_with_trade_update(self, trade_update: Dict[str, Any]) -> bool:
        """
        Updates the in flight order with trade update (from GET /trade_history end point)
        return: True if the order gets updated otherwise False
        """
        trade_id = trade_update["trade_id"]
        candidate_order_ids = {trade_update["buy_display_order_id"], trade_update["sell_display_order_id"]}
        if self.exchange_order_id not in candidate_order_ids or trade_id in self.trade_id_set:
            return False
        self.trade_id_set.add(trade_id)
        self.executed_amount_base += Decimal(str(trade_update["quantity"]))
        self.executed_amount_quote += Decimal(str(trade_update["price"])) * Decimal(str(trade_update["quantity"]))
        # self.fee_paid += Decimal(str(trade_update["fee_amount"])) # ToDo: trade updates don't carry fee info
        # if not self.fee_asset:
        #     self.fee_asset = trade_update["fee_currency_id"]
        return True
