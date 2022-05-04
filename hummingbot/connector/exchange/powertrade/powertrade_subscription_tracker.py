from hummingbot.connector.exchange.powertrade.powertrade_websocket_adaptor import (
    WebsocketResponseCode, IGNORABLE_ERROR_CODES)


class PowerTradeSubscriptionTracker:
    def __init__(self, trading_pairs):
        self.subscription_status = {}
        self.subscription_user_tags = {}
        self._trading_pairs = trading_pairs

    def process_control_message(self, message):
        msg_user_tag = message['command_response'].get('user_tag')
        error_code = int(message["command_response"]["error_code"])
        if WebsocketResponseCode(error_code) in IGNORABLE_ERROR_CODES:
            for trading_pair, user_tag in self.subscription_user_tags.items():
                if msg_user_tag == user_tag:
                    self.subscription_status[trading_pair] = True
                    return True
        return False

    def reset_subscription_status(self):
        self.subscription_status = {tp: False for tp in self._trading_pairs}
        self.subscription_user_tags = {tp: None for tp in self._trading_pairs}

    @property
    def ready(self):
        return all(status for status in self.subscription_status.values())
