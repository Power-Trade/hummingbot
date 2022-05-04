import os
from functools import partial

# A single source of truth for constant variables related to the exchange
import typing

EXCHANGE_NAME = "powertrade"

REST_API_VERSION = "v1"
REST_PORT = 4321

setting = os.getenv("POWERTRADE_PORT")
if setting is not None:
    REST_PORT = 4321

_domains = ["prod", "test"]

def _make_urls(pattern: str) -> typing.Dict[str, str]:
    res : typing.Dict[str, str] = dict()
    for t in _domains:
        res[t] = pattern.format(domain=t)
    return res


rest_url = _make_urls("https://proxy.{{domain}}.powertrade.dev:{port}".format(port=REST_PORT))
pts_url = _make_urls("https://api.rest.{domain}.power.trade")
uri = _make_urls("api.rest.{domain}.power.trade")
wss_url = _make_urls("ws://proxy.{{domain}}.powertrade.dev:{port}".format(port=REST_PORT))

# REST API Private Endpoints
ENTITIES_AND_RULES_URL = partial("{url}{rest_api_version}/api/exchangeInfo".format,
                                 rest_api_version=REST_API_VERSION)
ORDER_BOOK_URL = partial("{url}{rest_api_version}/api/orderbook".format,
                         rest_api_version=REST_API_VERSION)
TIME_URL = partial("{url}{rest_api_version}/api/time".format,
                   rest_api_version=REST_API_VERSION)
SUMMARY_24HS_URL = partial("{url}{rest_api_version}/market_data/tradeable_entity/all/summary".format,
                           rest_api_version=REST_API_VERSION)
NEW_ORDER_URL = partial("{url}{rest_api_version}/api/order".format,
                        rest_api_version=REST_API_VERSION)
CANCEL_ORDER_URL = partial("{url}{rest_api_version}/api/order".format,
                           rest_api_version=REST_API_VERSION)
QUERY_ORDER_URL = partial("{url}{rest_api_version}/api/order".format,
                          rest_api_version=REST_API_VERSION)
TRADE_HISTORY_URL = partial("{url}{rest_api_version}/reporting/balance_activity?update_reason=trade".format,
                            rest_api_version=REST_API_VERSION)
BALANCE_URL = partial("{url}{rest_api_version}/api/balance".format,
                      rest_api_version=REST_API_VERSION)
OPEN_ORDER_URL = partial("{url}{rest_api_version}/api/openOrders".format,
                         rest_api_version=REST_API_VERSION)
DELIVERABLES_URL = partial("{url}{rest_api_version}/api/deliverableInfo".format,
                         rest_api_version=REST_API_VERSION)

# Websocket Private Channels
WS_PRIVATE_CHANNELS = ["trade", "cancel_order", "trade_history"]
