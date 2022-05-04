#!/usr/bin/env python

import dateutil.parser as dp
import re
from datetime import datetime
from typing import (
    List,
    Tuple,
    Optional
)

from hummingbot.core.utils.tracking_nonce import get_tracking_nonce
from hummingbot.core.data_type.order_book_row import OrderBookRow
from hummingbot.core.data_type.order_book_message import OrderBookMessage

from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_methods import using_exchange

CENTRALIZED = True

EXAMPLE_PAIR = "ETH-USDT"

DEFAULT_FEES = [0.2, 0.2]

RE_OPTIONS = re.compile(r"^(\w+)-(\d+)-(\w+)$")
RE_SPOT = re.compile(r"^([a-zA-Z]+)-([a-zA-Z]+)$")


def get_new_client_order_id(is_buy: bool, trading_pair: str) -> str:
    side = "B" if is_buy else "S"
    return f"{side}-{trading_pair}-{get_tracking_nonce()}"


def convert_iso_to_epoch(ts: str) -> float:
    return dp.parse(ts).timestamp()


def get_iso_time_now() -> str:
    return datetime.utcnow().isoformat()[:-3] + 'Z'


def convert_snapshot_message_to_order_book_row(message: OrderBookMessage) -> Tuple[List[OrderBookRow], List[OrderBookRow]]:
    update_id = message.update_id
    data = message.content
    # if "data" in message.content:  # From REST API
    #     data: List[Dict[str, Any]] = message.content["data"]
    # elif "order_books" in message.content:  # From Websocket API
    #     data: List[Dict[str, Any]] = message.content["order_books"]
    bids, asks = [], []

    # for entry in data:
    #     order_row = OrderBookRow(float(entry["price"]), float(entry["quantity"]), update_id)
    #     if entry["side"] == "buy":
    #         bids.append(order_row)
    #     else:  # entry["type"] == "Sell":
    #         asks.append(order_row)
    for entry in data["buy"]:
        order_row = OrderBookRow(float(entry["price"]), float(entry["quantity"]), update_id)
        bids.append(order_row)
    for entry in data["sell"]:
        order_row = OrderBookRow(float(entry["price"]), float(entry["quantity"]), update_id)
        asks.append(order_row)

    return bids, asks


def convert_diff_message_to_order_book_row(message: OrderBookMessage) -> Tuple[List[OrderBookRow], List[OrderBookRow]]:
    update_id = message.update_id
    data = message.content
    bids = []
    asks = []

    for action, content in data["buy"].items():
        if action == "delete_price_level":
            bids.append(OrderBookRow(float(content), 0, update_id))
        if action == "add_price_level":
            for level in content:
                bids.append(OrderBookRow(float(level["price"]), float(level["quantity"]), update_id))
    for action, content in data["sell"].items():
        if action == "delete_price_level":
            asks.append(OrderBookRow(float(content), 0, update_id))
        if action == "add_price_level":
            for level in content:
                asks.append(OrderBookRow(float(level["price"]), float(level["quantity"]), update_id))

    return bids, asks


def split_trading_pair(trading_pair: str) -> Optional[Tuple[str, str]]:
    try:
        # m = RE_OPTIONS.match(trading_pair)
        m = RE_SPOT.match(trading_pair)
        if m:
            return m.group(1), m.group(2)
            # return m.group(1)+"."+m.group(2)+"."+m.group(3)
        return None
    except Exception as e:
        raise e


def convert_from_exchange_trading_pair(exchange_trading_pair: str, quote_asset: str = None) -> Optional[str]:
    if split_trading_pair(exchange_trading_pair) is None:
        return None
    # option_name = split_trading_pair(exchange_trading_pair)
    # return f"{option_name}-{quote_asset}"
    base_asset, quote_asset = split_trading_pair(exchange_trading_pair)
    return f"{base_asset}-{quote_asset}"


def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    # option_name, _ = hb_trading_pair.split("-")
    # return option_name.replace(".", "-")
    base_asset, quote_asset = hb_trading_pair.split("-")
    return f"{base_asset}-{quote_asset}"


# See https://docs.hummingbot.io/developer/task1/

KEYS = {
    "powertrade_api_key":
        ConfigVar(key="powertrade_api_key",
                  prompt="Enter your Powertrade Client ID >>> ",
                  required_if=using_exchange("powertrade"),
                  is_secure=True,
                  is_connect_key=True),
    "powertrade_secret_key":
        ConfigVar(key="powertrade_secret_key",
                  prompt="Enter your Powertrade secret key >>> ",
                  required_if=using_exchange("powertrade"),
                  is_secure=True,
                  is_connect_key=True,
                  type_str="rawstr"),
}

OTHER_DOMAINS = ["powertrade_test"]
OTHER_DOMAINS_PARAMETER = {"powertrade_test": "test"}
OTHER_DOMAINS_EXAMPLE_PAIR = {"powertrade_test": "BTC-USDT"}
OTHER_DOMAINS_DEFAULT_FEES = {"powertrade_test": [0.2, 0.2]}
OTHER_DOMAINS_KEYS = {"powertrade_test": {
    "powertrade_test_api_key":
        ConfigVar(key="powertrade_test_api_key",
                  prompt="Enter your Powertrade TEST Client ID >>> ",
                  required_if=using_exchange("powertrade_test"),
                  is_secure=True,
                  is_connect_key=True),
    "powertrade_test_secret_key":
        ConfigVar(key="powertrade_test_secret_key",
                  prompt="Enter your Powertrade TEST secret key >>> ",
                  required_if=using_exchange("powertrade_test"),
                  is_secure=True,
                  is_connect_key=True,
                  type_str="rawstr"),
}}
