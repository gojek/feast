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
from configparser import NoOptionError
from http import HTTPStatus
from unittest.mock import call, patch

from google.auth.exceptions import DefaultCredentialsError
from pytest import fixture, raises

from feast.config import Config
from feast.grpc.auth import (
    GoogleOpenIDAuthMetadataPlugin,
    OAuthMetadataPlugin,
    get_auth_metadata_plugin,
)

AUDIENCE = "https://testaudience.io/"

AUTH_URL = "https://test.auth.com/v2/token"

HEADERS = {"content-type": "application/json"}

DATA = json.dumps(
    {
        "grant_type": "client_credentials",
        "client_id": "fakeID",
        "client_secret": "fakeSecret",
        "audience": AUDIENCE,
    }
)


class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data


class GoogleMockResponse:
    def __init__(self, stdout):
        self.stdout = stdout


class GoogleDefaultResponse:
    def __init__(self, id_token):
        self.id_token = id_token

    def refresh(self, request):
        pass


class GoogleDefaultErrorResponse:
    def __init__(self, id_token):
        self.wrong_attribute = id_token

    def refresh(self, request):
        pass


@fixture
def config_oauth():
    config_dict = {
        "core_url": "localhost:50051",
        "enable_auth": True,
        "auth_provider": "oauth",
        "oauth_grant_type": "client_credentials",
        "oauth_client_id": "fakeID",
        "oauth_client_secret": "fakeSecret",
        "oauth_audience": AUDIENCE,
        "oauth_token_request_url": AUTH_URL,
    }
    return Config(config_dict)


@fixture
def config_google():
    config_dict = {
        "core_url": "localhost:50051",
        "enable_auth": True,
        "auth_provider": "google",
    }
    return Config(config_dict)


@fixture
def config_with_missing_variable():
    config_dict = {
        "core_url": "localhost:50051",
        "enable_auth": True,
        "auth_provider": "oauth",
        "oauth_grant_type": "client_credentials",
        "oauth_client_id": "fakeID",
        "oauth_client_secret": "fakeSecret",
        "oauth_token_request_url": AUTH_URL,
    }
    return Config(config_dict)


@patch(
    "requests.post",
    return_value=MockResponse(
        {"access_token": "mock_token", "expires_in": 2000}, HTTPStatus.OK
    ),
)
def test_get_auth_metadata_plugin_oauth_should_pass(post, config_oauth):
    auth_metadata_plugin = get_auth_metadata_plugin(config_oauth)
    assert isinstance(auth_metadata_plugin, OAuthMetadataPlugin)
    assert post.call_count == 1
    assert post.call_args == call(AUTH_URL, headers=HEADERS, data=DATA)
    assert auth_metadata_plugin.get_signed_meta() == (
        ("authorization", "Bearer mock_token"),
    )


@patch(
    "requests.post",
    return_value=MockResponse({"access_token": "mock_token"}, HTTPStatus.UNAUTHORIZED),
)
def test_get_auth_metadata_plugin_oauth_should_raise_when_response_is_not_200(
    post, config_oauth
):
    with raises(RuntimeError):
        get_auth_metadata_plugin(config_oauth)
        assert post.call_count == 1
        assert post.call_args == call(AUTH_URL, headers=HEADERS, data=DATA)


def test_get_auth_metadata_plugin_oauth_should_raise_when_config_is_incorrect(
    config_with_missing_variable,
):
    with raises((RuntimeError, NoOptionError)):
        get_auth_metadata_plugin(config_with_missing_variable)


@patch(
    "google.oauth2.id_token.verify_token",
    return_value={"iss": "accounts.google.com", "exp": 12341234},
)
@patch("google.oauth2.id_token.fetch_id_token", return_value="Some Token")
def test_get_auth_metadata_plugin_google_should_pass_with_token_from_gcloud_sdk(
    verify_token, fetch_id_token, config_google
):
    auth_metadata_plugin = get_auth_metadata_plugin(config_google)
    assert isinstance(auth_metadata_plugin, GoogleOpenIDAuthMetadataPlugin)
    assert auth_metadata_plugin.get_signed_meta() == (
        ("authorization", "Bearer Some Token"),
    )


@patch(
    "google.oauth2.id_token.verify_token",
    return_value={"iss": "accounts.google.com", "exp": 12341234},
)
@patch(
    "google.auth.default",
    return_value=[
        GoogleDefaultResponse("fake_token"),
        GoogleDefaultResponse("project_id"),
    ],
)
@patch("google.oauth2.id_token.fetch_id_token", side_effect=DefaultCredentialsError())
def test_get_auth_metadata_plugin_google_should_pass_with_token_from_google_auth_lib(
    verify_token, fetch_id_token, default, config_google
):
    auth_metadata_plugin = get_auth_metadata_plugin(config_google)
    assert isinstance(auth_metadata_plugin, GoogleOpenIDAuthMetadataPlugin)
    assert auth_metadata_plugin.get_signed_meta() == (
        ("authorization", "Bearer fake_token"),
    )


@patch(
    "google.auth.default",
    return_value=[
        GoogleDefaultErrorResponse("fake_token"),
        GoogleDefaultErrorResponse("project_id"),
    ],
)
@patch("google.oauth2.id_token.fetch_id_token", side_effect=DefaultCredentialsError())
def test_get_auth_metadata_plugin_google_should_raise_when_token_validation_fails(
    fetch_id_token, default, config_google
):
    with raises(RuntimeError):
        get_auth_metadata_plugin(config_google)
