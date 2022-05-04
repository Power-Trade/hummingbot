import asyncio
import logging
from enum import Enum
from typing import AsyncIterable, Dict, Any, List, Optional

import ujson
import aiohttp
import pandas as pd

import hummingbot.connector.exchange.powertrade.powertrade_constants as CONSTANTS
from hummingbot.connector.exchange.powertrade.powertrade_auth import PowertradeAuth
from hummingbot.logger import HummingbotLogger

powertrade_logger = None


class WebsocketResponseCode(Enum):
    SUCCESS = 0
    INVALID_SYMBOL = 1
    INVALID_INTERVAL = 2
    INVALID_SUBSCRIPTION_TYPE = 3
    SYMBOL_NOT_FOUND = 4
    SUBSCRIPTION_FAILED = 5
    UNSUBSCRIPTION_FAILED = 6
    INVALID_USER_TAG = 7
    EXCHANGE_DOWN = 8
    INVALID_CREDENTIALS = 9
    AUTH_FAILED = 10
    ALREADY_AUTHENTICATED = 11
    NOT_AUTHENTICATED = 12
    UNKNOWN_SUBSCRIPTION_TYPE = 13
    TRADES_SUBSCRIPTION_FAILED = 14
    TRADES_UNSUBSCRIPTION_FAILED = 15
    INVALID_PARAMETERS = 16
    NEW_ORDER_FAILED = 17
    NO_TRADE_HISTORY = 18
    HISTORY_FILE_OPEN_FAILED = 19
    HISTORY_FILE_SIZE_FAILED = 20
    HISTORY_FILE_FSEEK_FAILED = 21


class WebsocketControlMessage(Enum):
    WS_AUTH = 1
    TRACKER_SUBSCRIPTION = 2


IGNORABLE_ERROR_CODES = (WebsocketResponseCode.ALREADY_AUTHENTICATED,
                         WebsocketResponseCode.SUCCESS,
                         WebsocketResponseCode.NO_TRADE_HISTORY
                         )
IGNORABLE_ERROR_TEXTS = (
    'add subscription failed, already subscribed',
)


class PowertradeWebSocketAdaptor:
    """
    Auxiliary class that works as a wrapper of a low level websocket.
    :param trackers: List of trackers to dispatch messages to
    :param ws_auth: Credentials generator for Powertrade
    """
    MESSAGE_TIMEOUT = 60
    PING_TIMEOUT = 5.0
    RETRY_TIMEOUT = 20.0
    AUTH_TIMEOUT = 30.0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global powertrade_logger
        if powertrade_logger is None:
            powertrade_logger = logging.getLogger(__name__)
        return powertrade_logger

    def __init__(
        self,
        trackers: List[Any],
        ws_auth: PowertradeAuth = None,
        domain: str = "prod",
    ):
        self._lock = asyncio.Lock()
        self._trackers = trackers
        self._ws_auth: PowertradeAuth = ws_auth
        self._session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False))
        self._websocket: Optional[aiohttp.ClientWebSocketResponse] = None
        self.authenticated = False
        self.last_authentication_ts = -1
        self.domain = domain

    async def _create_websocket_connection(self) -> aiohttp.ClientWebSocketResponse:
        """
        Initialize WebSocket client
        """
        try:
            ws = await self._session.ws_connect(CONSTANTS.wss_url[self.domain], max_msg_size=0, timeout=self.MESSAGE_TIMEOUT)
            return ws
        except Exception as ex:
            self.logger().network(f"Unexpected error occurred during {CONSTANTS.EXCHANGE_NAME} WebSocket Connection "
                                  f"({ex})")
            raise ex

    async def send_request(self, payload: Dict[str, Any], escape_forward_slashes=True):
        str_payload = ujson.dumps(payload, escape_forward_slashes=escape_forward_slashes)
        # self.logger().info(f"SENT: {str_payload}")
        await self._websocket.send_str(str_payload)
        await asyncio.sleep(0.001)  # Make sure timestamps in ms between messages are different

    async def recv(self):
        raw_msg: aiohttp.WSMessage = await self._websocket.receive(timeout=self.MESSAGE_TIMEOUT)
        if raw_msg.type == aiohttp.WSMsgType.TEXT:
            message = raw_msg.data
            # self.logger().info(f"RECEIVED: {message}")
            return message
        else:
            self.logger().info(f"Unknown websocket message: {raw_msg}")

    async def send_heartbeat(self):
        await self.send_request({"heartbeat": {"timestamp": f"{int(pd.Timestamp.utcnow().timestamp() * 1e9)}"}})

    async def _authenticate(self):
        try:
            auth_payload: Dict[str, Any] = self._ws_auth.get_ws_auth_payload()
            self.last_authentication_ts = pd.Timestamp.utcnow().timestamp()
            await self.send_request(auth_payload, escape_forward_slashes=False)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().info("Error occurred when authenticating to websocket. ",
                               exc_info=True)
            raise

    async def _iter_messages(self) -> AsyncIterable[str]:
        while True:
            msg: str = await self.recv()
            yield msg

    async def _subscribe_to_channels(self):
        for tracker in self._trackers:
            await tracker.data_source.subscribe_to_channels(self)

    def _reset_subscriptions(self):
        for tracker in self._trackers:
            tracker.data_source.subscription_tracker.reset_subscription_status()

    async def close(self):
        self.logger().info("Closing Websocket...")
        await self._websocket.close()
        self.logger().info("Websocket closed successfully")
        self._websocket = None

    async def websocket_main_update_loop(self):

        self.logger().info("Invoking new instance of websocket_main_update_loop")
        last_msg = pd.Timestamp.utcnow().timestamp()
        while True:
            try:
                self.authenticated = False
                self._reset_subscriptions()
                self._websocket = await asyncio.wait_for(self._create_websocket_connection(),
                                                         timeout=self.MESSAGE_TIMEOUT)
                await self._authenticate()
                try:
                    async for raw_msg in self._iter_messages():
                        message = ujson.loads(raw_msg)
                        last_msg = pd.Timestamp.utcnow().timestamp()
                        self._detect_errors_in_ws_msg(message)
                        ws_control_message = self._handle_control_messages(message)
                        if ws_control_message is not None:
                            if ws_control_message == WebsocketControlMessage.WS_AUTH:
                                await self._subscribe_to_channels()
                            elif ws_control_message == WebsocketControlMessage.TRACKER_SUBSCRIPTION:
                                self.logger().info(f"Successfully subscribed to datasource: "
                                                   f"{message['command_response']['user_tag']}")
                            continue
                        if await self._handle_heartbeats(message):
                            continue
                        if (not self.authenticated) and \
                                (pd.Timestamp.utcnow().timestamp() - self.last_authentication_ts > self.MESSAGE_TIMEOUT):
                            self.logger().info("Timed out trying to authenticate to PowerTrade WS.")
                            raise Exception("Timed out trying to authenticate to PowerTrade WS.")
                        for tracker in self._trackers:
                            if tracker.is_eligible_for_message(message):
                                tracker.process_message(message)
                except asyncio.TimeoutError:
                    self.logger().info(f"Timeout error in ws connection, retry or increasing the read timeout value {pd.Timestamp.utcnow().timestamp() - last_msg}")
                    await self._handle_timeout_in_ws()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network("Unexpected error with WebSocket connection.",
                                      exc_info=True,
                                      app_warning_msg=f"Unexpected error with WebSocket connection. "
                                                      f"Retrying in {self.RETRY_TIMEOUT} seconds. "
                                                      f"Check network connection.")
            finally:
                if self._websocket:
                    await self.close()
                await asyncio.sleep(self.RETRY_TIMEOUT)

    async def _handle_timeout_in_ws(self):
        pong_waiter = self._websocket.ping()
        await asyncio.wait_for(pong_waiter, timeout=self.PING_TIMEOUT)

    async def _handle_heartbeats(self, message) -> bool:
        if 'heartbeat' in message:
            await self.send_heartbeat()
            return True
        return False

    def _detect_errors_in_ws_msg(self, message):
        if 'error' in message:
            raise Exception(f"Websocket failure: {message['error']}")
        if 'command_response' in message:
            error_code = int(message["command_response"]["error_code"])
            error_text = message["command_response"]["error_text"]
            if (WebsocketResponseCode(error_code) not in IGNORABLE_ERROR_CODES) and \
                    all(err_text != error_text for err_text in IGNORABLE_ERROR_TEXTS):
                self.logger().error(f"{message['command_response']}",
                                    exc_info=True)
                raise Exception(f"{message['command_response']}")
            elif WebsocketResponseCode(error_code) != WebsocketResponseCode.SUCCESS:
                # Log this as if it could help diagnose websocket errors
                self.logger().info(f"{message['command_response']}")

    def _handle_control_messages(self, message) -> Optional[WebsocketControlMessage]:
        if 'command_response' not in message:
            return
        user_tag = message['command_response'].get('user_tag')
        error_code = int(message["command_response"]["error_code"])
        if user_tag:
            if user_tag == self._ws_auth.user_tag:
                if WebsocketResponseCode(error_code) in IGNORABLE_ERROR_CODES:
                    self.authenticated = True
                    self.logger().info("PowerTrade websocket successfully authenticated.")
                    return WebsocketControlMessage.WS_AUTH
            for tracker in self._trackers:
                if tracker.process_control_message(message):
                    return WebsocketControlMessage.TRACKER_SUBSCRIPTION
