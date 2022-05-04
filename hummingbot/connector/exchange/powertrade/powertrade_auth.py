#!/usr/bin/env python

import jwt
import pandas as pd
from typing import (
    Dict,
    Any
)


class PowertradeAuth:
    """
    Auth class required by PowerTrade API
    Learn more at https://docs.api.power.trade/#Authentication
    """

    # Internally authenticated sessions last for 30 seconds, but proxy will cache it for 5 minutes
    PROXY_AUTH_CACHE_TIMEOUT = 270

    def __init__(self, api_key: str, secret_key: str, uri: str):
        self.api_key: str = api_key
        self.secret_key: str = secret_key
        self.uri = uri

        self._credential_secret: str = None
        self._credential_secret_expiration_time: int = -1
        self._user_tag_used_to_authenticate = None

    @property
    def credential_secret(self):
        return self._credential_secret

    @property
    def user_tag(self):
        return self._user_tag_used_to_authenticate

    def _calculate_credential_secret_payload(self):
        now = int(pd.Timestamp.utcnow().timestamp())
        payload = {
            "client": "api",
            "uri": self.uri,
            "nonce": now,
            "iat": now,
            "exp": now + self.PROXY_AUTH_CACHE_TIMEOUT + 30,
            "sub": self.api_key
        }
        self._credential_secret_expiration_time = now + self.PROXY_AUTH_CACHE_TIMEOUT
        self._credential_secret = jwt.encode(payload, self.secret_key, algorithm="ES256").decode()

    @property
    def credential_secret_has_expired(self):
        now: int = int(pd.Timestamp.utcnow().timestamp())
        return now >= self._credential_secret_expiration_time

    def get_auth_headers(self):
        if self.credential_secret_has_expired:
            self._calculate_credential_secret_payload()

        return self._generate_auth_dict()

    def _generate_auth_dict(self):
        """
        Generates authentication signature and return it in a dictionary along with other inputs
        :return: a dictionary of request info including the request signature
        """
        headers = self.get_headers()
        headers.update({
            "credentials_secret": f"{self.credential_secret}"
        })

        return headers

    def get_headers(self) -> Dict[str, Any]:
        """
        Generates authentication headers required by PowerTrade
        :return: a dictionary of auth headers
        """

        return {
            "Content-Type": 'application/json',
        }

    def get_ws_auth_payload(self) -> Dict[str, Any]:
        if self.credential_secret_has_expired:
            self._calculate_credential_secret_payload()
        self._user_tag_used_to_authenticate = f"hummingbot_ws_auth_{int(pd.Timestamp.utcnow().timestamp()*1e9)}"
        return {"authenticate": {"credentials_secret": f"{self.credential_secret}",
                                 "user_tag": self._user_tag_used_to_authenticate}}

    def reset_credentials_expiration_timer(self):
        self._credential_secret_expiration_time = -1

    def is_auth_failed_message(self, message):
        if "error" in message:
            return message["error"].get("message", "") == "authorisation failed"
        return False
