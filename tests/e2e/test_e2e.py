import pytest
import math
import random
import time
from feast.entity import Entity
from feast.serving.ServingService_pb2 import (
    GetOnlineFeaturesRequest,
    GetOnlineFeaturesResponse,
)
from feast.types.Value_pb2 import Value as Value
from feast.client import Client
from feast.feature_set import FeatureSet
from feast.type_map import ValueType
from google.protobuf.duration_pb2 import Duration
from datetime import datetime
import pytz

import pandas as pd
import numpy as np

from feast.feature import Feature

FLOAT_TOLERANCE = 0.00001


@pytest.fixture(scope='session')
def core_url(pytestconfig):
    return pytestconfig.getoption("core_url")


@pytest.fixture(scope='session')
def serving_url(pytestconfig):
    return pytestconfig.getoption("serving_url")


@pytest.fixture(scope='session')
def allow_dirty(pytestconfig):
    return True if pytestconfig.getoption(
        "allow_dirty").lower() == "true" else False


@pytest.fixture(scope='session')
def client(core_url, serving_url, allow_dirty):
    # Get client for core and serving
    client = Client(core_url=core_url, serving_url=serving_url)

    # Ensure Feast core is active, but empty
    if not allow_dirty:
        feature_sets = client.list_feature_sets()
        if len(feature_sets) > 0:
            raise Exception(
                "Feast cannot have existing feature sets registered. Exiting tests."
            )

    return client


@pytest.fixture()
def basic_dataframe():
    offset = random.randint(1000, 100000)  # ensure a unique key space is used
    return pd.DataFrame(
    {
        "datetime": [datetime.utcnow().replace(tzinfo=pytz.utc) for _ in
                     range(5)],
        "customer_id": [offset + inc for inc in range(5)],
        "daily_transactions": [np.random.rand() for _ in range(5)],
        "total_transactions": [512 for _ in range(5)],
    }
    )


@pytest.mark.timeout(30)
def test_basic_register_feature_set_success(client):
    # Load feature set from file
    cust_trans_fs = FeatureSet.from_yaml("basic/cust_trans_fs.yaml")

    # Register feature set
    client.apply(cust_trans_fs)

    # Feast Core needs some time to fully commit the FeatureSet applied
    # when there is no existing job yet for the Featureset
    time.sleep(15)

    cust_trans_fs_applied = client.get_feature_set(name="customer_transactions")

    if cust_trans_fs_applied is None:
        raise Exception(
            "Client cannot retrieve 'customer_transactions' FeatureSet "
            "after registration. Either Feast Core does not save the "
            "FeatureSet correctly or the client needs to wait longer for FeatureSet "
            "to be committed."
        )

    assert cust_trans_fs_applied == cust_trans_fs


@pytest.mark.timeout(30)
def test_basic_ingest_success(client, basic_dataframe):
    cust_trans_fs = client.get_feature_set(name="customer_transactions")

    # Ingest customer transaction data
    client.ingest(cust_trans_fs, dataframe=basic_dataframe)


@pytest.mark.timeout(30)
def test_basic_retrieve_online_success(client, basic_dataframe):
    # Poll serving for feature values until the correct values are returned
    while True:
        time.sleep(1)

        response = client.get_online_features(
            entity_rows=[
                GetOnlineFeaturesRequest.EntityRow(
                    fields={
                        "customer_id": Value(
                            int64_val=basic_dataframe.iloc[0]["customer_id"]
                        )
                    }
                )
            ],
            feature_ids=[
                "customer_transactions:1:daily_transactions",
                "customer_transactions:1:total_transactions",
            ],
        )  # type: GetOnlineFeaturesResponse

        if response is None:
            continue

        returned_daily_transactions = float(
            response.field_values[0]
                .fields["customer_transactions:1:daily_transactions"]
                .float_val
        )
        sent_daily_transactions = float(
            basic_dataframe.iloc[0]["daily_transactions"])

        if math.isclose(
            sent_daily_transactions,
            returned_daily_transactions,
            abs_tol=FLOAT_TOLERANCE,
        ):
            break


@pytest.fixture()
def all_types_dataframe():
    return pd.DataFrame(
        {
            "datetime": [datetime.utcnow().replace(tzinfo=pytz.utc) for _ in
                         range(3)],
            "user_id": [1001, 1002, 1003],
            "int32_feature": [np.int32(1), np.int32(2), np.int32(3)],
            "int64_feature": [np.int64(1), np.int64(2), np.int64(3)],
            "float_feature": [np.float(0.1), np.float(0.2), np.float(0.3)],
            "double_feature": [np.float64(0.1), np.float64(0.2),
                               np.float64(0.3)],
            "string_feature": ["one", "two", "three"],
            "bytes_feature": [b"one", b"two", b"three"],
            "bool_feature": [True, False, False],
            "int32_list_feature": [
                np.array([1, 2, 3, 4], dtype=np.int32),
                np.array([1, 2, 3, 4], dtype=np.int32),
                np.array([1, 2, 3, 4], dtype=np.int32),
            ],
            "int64_list_feature": [
                np.array([1, 2, 3, 4], dtype=np.int64),
                np.array([1, 2, 3, 4], dtype=np.int64),
                np.array([1, 2, 3, 4], dtype=np.int64),
            ],
            "float_list_feature": [
                np.array([1.1, 1.2, 1.3, 1.4], dtype=np.float32),
                np.array([1.1, 1.2, 1.3, 1.4], dtype=np.float32),
                np.array([1.1, 1.2, 1.3, 1.4], dtype=np.float32),
            ],
            "double_list_feature": [
                np.array([1.1, 1.2, 1.3, 1.4], dtype=np.float64),
                np.array([1.1, 1.2, 1.3, 1.4], dtype=np.float64),
                np.array([1.1, 1.2, 1.3, 1.4], dtype=np.float64),
            ],
            "string_list_feature": [
                np.array(["one", "two", "three"]),
                np.array(["one", "two", "three"]),
                np.array(["one", "two", "three"]),
            ],
            "bytes_list_feature": [
                np.array([b"one", b"two", b"three"]),
                np.array([b"one", b"two", b"three"]),
                np.array([b"one", b"two", b"three"]),
            ],
            "bool_list_feature": [
                np.array([True, False, True]),
                np.array([True, False, True]),
                np.array([True, False, True]),
            ],
        }
    )


@pytest.mark.timeout(30)
@pytest.mark.run(order=1)
def test_all_types_register_feature_set_success(client):
    all_types_fs = FeatureSet(
        name="all_types",
        entities=[Entity(name="user_id", dtype=ValueType.INT64)],
        features=[
            Feature(name="float_feature", dtype=ValueType.FLOAT),
            Feature(name="int64_feature", dtype=ValueType.INT64),
            Feature(name="int32_feature", dtype=ValueType.INT32),
            Feature(name="string_feature", dtype=ValueType.STRING),
            Feature(name="bytes_feature", dtype=ValueType.BYTES),
            Feature(name="bool_feature", dtype=ValueType.BOOL),
            Feature(name="double_feature", dtype=ValueType.DOUBLE),
            Feature(name="float_list_feature", dtype=ValueType.FLOAT_LIST),
            Feature(name="int64_list_feature", dtype=ValueType.INT64_LIST),
            Feature(name="int32_list_feature", dtype=ValueType.INT32_LIST),
            Feature(name="string_list_feature",
                    dtype=ValueType.STRING_LIST),
            Feature(name="bytes_list_feature", dtype=ValueType.BYTES_LIST),
            Feature(name="bool_list_feature", dtype=ValueType.BOOL_LIST),
            Feature(name="double_list_feature",
                    dtype=ValueType.DOUBLE_LIST),
        ],
        max_age=Duration(seconds=3600),
    )

    # Register feature set
    client.apply(all_types_fs)

    # Feast Core needs some time to fully commit the FeatureSet applied
    # when there is no existing job yet for the Featureset
    time.sleep(10)

    all_types_fs_applied = client.get_feature_set(name="all_types")

    if all_types_fs is None:
        raise Exception(
            "Client cannot retrieve 'all_types_fs' FeatureSet "
            "after registration. Either Feast Core does not save the "
            "FeatureSet correctly or the client needs to wait longer for FeatureSet "
            "to be committed."
        )

    assert all_types_fs == all_types_fs_applied


@pytest.mark.timeout(300)
def test_all_types_ingest_success(client, all_types_dataframe):
    # Get all_types feature set
    all_types_fs = client.get_feature_set(name="all_types")

    # Ingest user embedding data
    client.ingest(all_types_fs, dataframe=all_types_dataframe)


@pytest.mark.timeout(300)
def test_all_types_retrieve_online_success(client, all_types_dataframe):

    # Poll serving for feature values until the correct values are returned
    while True:
        time.sleep(1)

        response = client.get_online_features(
            entity_rows=[
                GetOnlineFeaturesRequest.EntityRow(
                    fields={"user_id": Value(
                        int64_val=all_types_dataframe.iloc[0]["user_id"])}
                )
            ],
            feature_ids=[
                "all_types:1:float_feature",
                "all_types:1:int64_feature",
                "all_types:1:int32_feature",
                "all_types:1:string_feature",
                "all_types:1:bytes_feature",
                "all_types:1:bool_feature",
                "all_types:1:double_feature",
                "all_types:1:float_list_feature",
                "all_types:1:int64_list_feature",
                "all_types:1:int32_list_feature",
                "all_types:1:string_list_feature",
                "all_types:1:bytes_list_feature",
                "all_types:1:bool_list_feature",
                "all_types:1:double_list_feature",
            ],
        )  # type: GetOnlineFeaturesResponse

        if response is None:
            continue

        returned_float_list = (
            response.field_values[0]
                .fields["all_types:1:float_list_feature"]
                .float_list_val.val
        )

        sent_float_list = all_types_dataframe.iloc[0]["float_list_feature"]

        if math.isclose(
            returned_float_list[0], sent_float_list[0], abs_tol=FLOAT_TOLERANCE
        ):
            break


@pytest.fixture()
def large_volume_dataframe():
    ROW_COUNT = 50000
    offset = random.randint(1000000, 10000000)  # ensure a unique key space
    customer_data = pd.DataFrame(
        {
            "datetime": [
                datetime.utcnow().replace(tzinfo=pytz.utc) for _ in
                range(ROW_COUNT)
            ],
            "customer_id": [offset + inc for inc in range(ROW_COUNT)],
            "daily_transactions": [np.random.rand() for _ in range(ROW_COUNT)],
            "total_transactions": [256 for _ in range(ROW_COUNT)],
        }
    )
    return customer_data


@pytest.mark.timeout(30)
@pytest.mark.run(order=1)
def test_large_volume_register_feature_set_success(client):
    cust_trans_fs = FeatureSet.from_yaml(
        "large_volume/cust_trans_large_fs.yaml")

    # Register feature set
    client.apply(cust_trans_fs)

    # Feast Core needs some time to fully commit the FeatureSet applied
    # when there is no existing job yet for the Featureset
    time.sleep(10)
    cust_trans_fs_applied = client.get_feature_set(name="customer_transactions_large")

    if cust_trans_fs is None:
        raise Exception(
            "Client cannot retrieve 'customer_transactions' FeatureSet "
            "after registration. Either Feast Core does not save the "
            "FeatureSet correctly or the client needs to wait longer for FeatureSet "
            "to be committed."
        )

    assert cust_trans_fs_applied == cust_trans_fs


@pytest.mark.timeout(30)
@pytest.mark.run(order=2)
def test_large_volume_ingest_success(client, large_volume_dataframe):

    # Get large volume feature set
    cust_trans_fs = client.get_feature_set(name="customer_transactions_large")
    
    # Ingest customer transaction data
    client.ingest(cust_trans_fs, dataframe=large_volume_dataframe)


@pytest.mark.timeout(20)
@pytest.mark.run(order=3)
def test_large_volume_retrieve_online_success(client, large_volume_dataframe):
    # Poll serving for feature values until the correct values are returned
    while True:
        time.sleep(1)

        response = client.get_online_features(
            entity_rows=[
                GetOnlineFeaturesRequest.EntityRow(
                    fields={
                        "customer_id": Value(
                            int64_val=large_volume_dataframe.iloc[0][
                                "customer_id"]
                        )
                    }
                )
            ],
            feature_ids=[
                "customer_transactions_large:1:daily_transactions",
                "customer_transactions_large:1:total_transactions",
            ],
        )  # type: GetOnlineFeaturesResponse

        if response is None:
            continue

        returned_daily_transactions = float(
            response.field_values[0]
                .fields["customer_transactions_large:1:daily_transactions"]
                .float_val
        )
        sent_daily_transactions = float(
            large_volume_dataframe.iloc[0]["daily_transactions"])

        if math.isclose(
            sent_daily_transactions,
            returned_daily_transactions,
            abs_tol=FLOAT_TOLERANCE,
        ):
            break
