# SPDX-License-Identifier: Apache-2.0
# Copyright 2018-2020 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
import time
from http import HTTPStatus

import grpc
import requests
from google.auth.exceptions import DefaultCredentialsError
from google.auth.transport import requests as grequests

from feast.config import Config
from feast.constants import AuthProvider
from feast.constants import ConfigOptions as opt


def get_auth_metadata_plugin(config: Config) -> grpc.AuthMetadataPlugin:
    """
    Get an Authentication Metadata Plugin. This plugin is used in gRPC to
    sign requests. Please see the following URL for more details
    https://grpc.github.io/grpc/python/_modules/grpc.html#AuthMetadataPlugin

    New plugins can be added to this function. For the time being we only
    support Google Open ID authentication.

    Returns: Returns an implementation of grpc.AuthMetadataPlugin

    Args:
        config: Feast Configuration object
    """
    if AuthProvider(config.get(opt.AUTH_PROVIDER)) == AuthProvider.GOOGLE:
        return GoogleOpenIDAuthMetadataPlugin(config)
    elif AuthProvider(config.get(opt.AUTH_PROVIDER)) == AuthProvider.OAUTH:
        return OAuthMetadataPlugin(config)
    else:
        raise RuntimeError(
            "Could not determine OAuth provider."
            'Must be set to either "google" or "oauth"'
        )


class OAuthMetadataPlugin(grpc.AuthMetadataPlugin):
    """A `gRPC AuthMetadataPlugin`_ that inserts the credentials into each
    request.

    .. _gRPC AuthMetadataPlugin:
        http://www.grpc.io/grpc/python/grpc.html#grpc.AuthMetadataPlugin
    """

    def __init__(self, config: Config):
        """
        Initializes an OAuthMetadataPlugin, used to sign gRPC requests
        Args:
            config: Feast Configuration object
        """
        super(OAuthMetadataPlugin, self).__init__()

        self._static_token = None
        self._token = None
        self._config = config
        self._token_expiry_ts = 0

        # If provided, set a static token
        if config.exists(opt.AUTH_TOKEN):
            self._static_token = config.get(opt.AUTH_TOKEN)
            self._refresh_token(config)
        elif (
            config.exists(opt.OAUTH_GRANT_TYPE)
            and config.exists(opt.OAUTH_CLIENT_ID)
            and config.exists(opt.OAUTH_CLIENT_SECRET)
            and config.exists(opt.OAUTH_AUDIENCE)
            and config.exists(opt.OAUTH_TOKEN_REQUEST_URL)
        ):
            self._refresh_token(config)
        else:
            raise RuntimeError(
                " Please ensure that the "
                "necessary parameters are passed to the client - "
                "oauth_grant_type, oauth_client_id, oauth_client_secret, "
                "oauth_audience, oauth_token_request_url."
            )

    def get_signed_meta(self):
        """ Creates a signed authorization metadata token."""

        current_time = time.time()
        if current_time > (self._token_expiry_ts - 500):
            self._refresh_token(self._config)
            self._token_expiry_ts = current_time + self._token_expiry_ts
        return (("authorization", "Bearer {}".format(self._token)),)

    def _refresh_token(self, config: Config):
        """ Refreshes OAuth token and persists it in memory """

        # Use static token if available
        if self._static_token:
            self._token = self._static_token
            return

        headers_token = {"content-type": "application/json"}
        data_token_dict = {
            "grant_type": config.get(opt.OAUTH_GRANT_TYPE),
            "client_id": config.get(opt.OAUTH_CLIENT_ID),
            "client_secret": config.get(opt.OAUTH_CLIENT_SECRET),
            "audience": config.get(opt.OAUTH_AUDIENCE),
        }
        data_token = json.dumps(data_token_dict)
        response_token = requests.post(
            config.get(opt.OAUTH_TOKEN_REQUEST_URL),
            headers=headers_token,
            data=data_token,
        )
        if response_token.status_code == HTTPStatus.OK:
            self._token = response_token.json().get("access_token")
            self._token_expiry_ts = response_token.json().get("expires_in")
        else:
            raise RuntimeError(
                f"Could not fetch OAuth token, got response : {response_token.status_code}"
            )

    def set_static_token(self, token):
        """
        Define a static token to return

        Args:
            token: String token
        """
        self._static_token = token

    def __call__(self, context, callback):
        """Passes authorization metadata into the given callback.

        Args:
            context (grpc.AuthMetadataContext): The RPC context.
            callback (grpc.AuthMetadataPluginCallback): The callback that will
                be invoked to pass in the authorization metadata.
        """
        callback(self.get_signed_meta(), None)


class GoogleOpenIDAuthMetadataPlugin(grpc.AuthMetadataPlugin):
    """A `gRPC AuthMetadataPlugin`_ that inserts the credentials into each
    request.

    .. _gRPC AuthMetadataPlugin:
        http://www.grpc.io/grpc/python/grpc.html#grpc.AuthMetadataPlugin
    """

    def __init__(self, config: Config):
        """
        Initializes a GoogleOpenIDAuthMetadataPlugin, used to sign gRPC requests
        Args:
            config: Feast Configuration object
        """
        super(GoogleOpenIDAuthMetadataPlugin, self).__init__()

        self._static_token = None
        self._token = None
        self._token_expiry_ts = time.time()

        # If provided, set a static token
        if config.exists(opt.AUTH_TOKEN):
            self._static_token = config.get(opt.AUTH_TOKEN)

        self._request = RequestWithTimeout(timeout=5)
        self._refresh_token()

    def get_signed_meta(self):
        """ Creates a signed authorization metadata token."""

        if time.time() > self._token_expiry_ts:
            self._refresh_token()
        return (("authorization", "Bearer {}".format(self._token)),)

    def _refresh_token(self):
        """ Refreshes Google ID token and persists it in memory """

        # Use static token if available
        if self._static_token:
            self._token = self._static_token
            return

        from google.oauth2.id_token import fetch_id_token, verify_oauth2_token

        try:
            self._token = fetch_id_token(self._request, audience="feast.dev")
            self._token_expiry_ts = verify_oauth2_token(self._token, self._request)[
                "exp"
            ]
            return
        except DefaultCredentialsError:
            pass

        # Try to use Google Auth library to find ID Token
        from google import auth as google_auth

        try:
            credentials, _ = google_auth.default(["openid", "email"])
            credentials.refresh(self._request)
            if hasattr(credentials, "id_token"):
                self._token = credentials.id_token
                self._token_expiry_ts = verify_oauth2_token(self._token, self._request)[
                    "exp"
                ]
                return
        except DefaultCredentialsError:
            pass  # Could not determine credentials, skip

        # Raise exception otherwise
        raise RuntimeError(
            "Could not determine Google ID token. Ensure that a service account can be found by setting"
            " the GOOGLE_APPLICATION_CREDENTIALS environmental variable to its path."
        )

    def set_static_token(self, token):
        """
        Define a static token to return

        Args:
            token: String token
        """
        self._static_token = token

    def __call__(self, context, callback):
        """Passes authorization metadata into the given callback.

        Args:
            context (grpc.AuthMetadataContext): The RPC context.
            callback (grpc.AuthMetadataPluginCallback): The callback that will
                be invoked to pass in the authorization metadata.
        """
        callback(self.get_signed_meta(), None)


class RequestWithTimeout(grequests.Request):
    def __init__(self, *args, timeout=None, **kwargs):
        self._timeout = timeout
        super().__init__(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        timeout = kwargs.pop("timeout", self._timeout)
        return super().__call__(*args, timeout=timeout, **kwargs)
