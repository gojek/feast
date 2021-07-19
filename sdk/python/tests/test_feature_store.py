# Copyright 2019 The Feast Authors
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
import time
from datetime import datetime, timedelta
from tempfile import mkstemp

import pytest
from pytest_lazyfixture import lazy_fixture

from feast import FileSource
from feast.data_format import ParquetFormat
from feast.entity import Entity
from feast.feature import Feature
from feast.feature_store import FeatureStore
from feast.feature_view import FeatureView
from feast.infra.offline_stores.file import FileOfflineStoreConfig
from feast.infra.online_stores.dynamodb import DynamoDBOnlineStoreConfig
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.protos.feast.types import Value_pb2 as ValueProto
from feast.repo_config import RepoConfig
from feast.value_type import ValueType
from tests.utils.data_source_utils import (
    prep_file_source,
    simple_bq_source_using_query_arg,
    simple_bq_source_using_table_ref_arg,
)


@pytest.fixture
def feature_store_with_local_registry():
    fd, registry_path = mkstemp()
    fd, online_store_path = mkstemp()
    return FeatureStore(
        config=RepoConfig(
            registry=registry_path,
            project="default",
            provider="local",
            online_store=SqliteOnlineStoreConfig(path=online_store_path),
        )
    )


@pytest.fixture
def feature_store_with_gcs_registry():
    from google.cloud import storage

    storage_client = storage.Client()
    bucket_name = f"feast-registry-test-{int(time.time() * 1000)}"
    bucket = storage_client.bucket(bucket_name)
    bucket = storage_client.create_bucket(bucket)
    bucket.add_lifecycle_delete_rule(
        age=14
    )  # delete buckets automatically after 14 days
    bucket.patch()
    bucket.blob("registry.db")

    return FeatureStore(
        config=RepoConfig(
            registry=f"gs://{bucket_name}/registry.db",
            project="default",
            provider="gcp",
        )
    )


@pytest.fixture
def feature_store_with_s3_registry():
    return FeatureStore(
        config=RepoConfig(
            registry=f"s3://feast-integration-tests/registries/{int(time.time() * 1000)}/registry.db",
            project="default",
            provider="aws",
            online_store=DynamoDBOnlineStoreConfig(region="us-west-2"),
            offline_store=FileOfflineStoreConfig(),
        )
    )


@pytest.mark.parametrize(
    "test_feature_store", [lazy_fixture("feature_store_with_local_registry")],
)
def test_apply_entity_success(test_feature_store):
    entity = Entity(
        name="driver_car_id",
        description="Car driver id",
        value_type=ValueType.STRING,
        labels={"team": "matchmaking"},
    )

    # Register Entity
    test_feature_store.apply(entity)

    entities = test_feature_store.list_entities()

    entity = entities[0]
    assert (
        len(entities) == 1
        and entity.name == "driver_car_id"
        and entity.value_type == ValueType(ValueProto.ValueType.STRING)
        and entity.description == "Car driver id"
        and "team" in entity.labels
        and entity.labels["team"] == "matchmaking"
    )

    test_feature_store.teardown()


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_feature_store",
    [
        lazy_fixture("feature_store_with_gcs_registry"),
        lazy_fixture("feature_store_with_s3_registry"),
    ],
)
def test_apply_entity_integration(test_feature_store):
    entity = Entity(
        name="driver_car_id",
        description="Car driver id",
        value_type=ValueType.STRING,
        labels={"team": "matchmaking"},
    )

    # Register Entity
    test_feature_store.apply([entity])

    entities = test_feature_store.list_entities()

    entity = entities[0]
    assert (
        len(entities) == 1
        and entity.name == "driver_car_id"
        and entity.value_type == ValueType(ValueProto.ValueType.STRING)
        and entity.description == "Car driver id"
        and "team" in entity.labels
        and entity.labels["team"] == "matchmaking"
    )

    entity = test_feature_store.get_entity("driver_car_id")
    assert (
        entity.name == "driver_car_id"
        and entity.value_type == ValueType(ValueProto.ValueType.STRING)
        and entity.description == "Car driver id"
        and "team" in entity.labels
        and entity.labels["team"] == "matchmaking"
    )

    test_feature_store.teardown()


@pytest.mark.parametrize(
    "test_feature_store", [lazy_fixture("feature_store_with_local_registry")],
)
def test_apply_feature_view_success(test_feature_store):
    # Create Feature Views
    batch_source = FileSource(
        file_format=ParquetFormat(),
        file_url="file://feast/*",
        event_timestamp_column="ts_col",
        created_timestamp_column="timestamp",
        date_partition_column="date_partition_col",
    )

    fv1 = FeatureView(
        name="my_feature_view_1",
        features=[
            Feature(name="fs1_my_feature_1", dtype=ValueType.INT64),
            Feature(name="fs1_my_feature_2", dtype=ValueType.STRING),
            Feature(name="fs1_my_feature_3", dtype=ValueType.STRING_LIST),
            Feature(name="fs1_my_feature_4", dtype=ValueType.BYTES_LIST),
        ],
        entities=["fs1_my_entity_1"],
        tags={"team": "matchmaking"},
        input=batch_source,
        ttl=timedelta(minutes=5),
    )

    # Register Feature View
    test_feature_store.apply([fv1])

    feature_views = test_feature_store.list_feature_views()

    # List Feature Views
    assert (
        len(feature_views) == 1
        and feature_views[0].name == "my_feature_view_1"
        and feature_views[0].features[0].name == "fs1_my_feature_1"
        and feature_views[0].features[0].dtype == ValueType.INT64
        and feature_views[0].features[1].name == "fs1_my_feature_2"
        and feature_views[0].features[1].dtype == ValueType.STRING
        and feature_views[0].features[2].name == "fs1_my_feature_3"
        and feature_views[0].features[2].dtype == ValueType.STRING_LIST
        and feature_views[0].features[3].name == "fs1_my_feature_4"
        and feature_views[0].features[3].dtype == ValueType.BYTES_LIST
        and feature_views[0].entities[0] == "fs1_my_entity_1"
    )

    test_feature_store.teardown()


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_feature_store", [lazy_fixture("feature_store_with_local_registry")],
)
@pytest.mark.parametrize("dataframe_source", [lazy_fixture("simple_dataset_1")])
def test_feature_view_inference_success(test_feature_store, dataframe_source):
    with prep_file_source(
        df=dataframe_source, event_timestamp_column="ts_1"
    ) as file_source:
        fv1 = FeatureView(
            name="fv1",
            entities=["id"],
            ttl=timedelta(minutes=5),
            online=True,
            input=file_source,
            tags={},
        )

        fv2 = FeatureView(
            name="fv2",
            entities=["id"],
            ttl=timedelta(minutes=5),
            online=True,
            input=simple_bq_source_using_table_ref_arg(dataframe_source, "ts_1"),
            tags={},
        )

        fv3 = FeatureView(
            name="fv3",
            entities=["id"],
            ttl=timedelta(minutes=5),
            online=True,
            input=simple_bq_source_using_query_arg(dataframe_source, "ts_1"),
            tags={},
        )

        test_feature_store.apply([fv1, fv2, fv3])  # Register Feature Views
        feature_view_1 = test_feature_store.list_feature_views()[0]
        feature_view_2 = test_feature_store.list_feature_views()[1]
        feature_view_3 = test_feature_store.list_feature_views()[2]

        actual_file_source = {
            (feature.name, feature.dtype) for feature in feature_view_1.features
        }
        actual_bq_using_table_ref_arg_source = {
            (feature.name, feature.dtype) for feature in feature_view_2.features
        }
        actual_bq_using_query_arg_source = {
            (feature.name, feature.dtype) for feature in feature_view_3.features
        }
        expected = {
            ("float_col", ValueType.DOUBLE),
            ("int64_col", ValueType.INT64),
            ("string_col", ValueType.STRING),
        }

        assert (
            expected
            == actual_file_source
            == actual_bq_using_table_ref_arg_source
            == actual_bq_using_query_arg_source
        )

        test_feature_store.teardown()


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_feature_store",
    [
        lazy_fixture("feature_store_with_gcs_registry"),
        lazy_fixture("feature_store_with_s3_registry"),
    ],
)
def test_apply_feature_view_integration(test_feature_store):
    # Create Feature Views
    batch_source = FileSource(
        file_format=ParquetFormat(),
        file_url="file://feast/*",
        event_timestamp_column="ts_col",
        created_timestamp_column="timestamp",
        date_partition_column="date_partition_col",
    )

    fv1 = FeatureView(
        name="my_feature_view_1",
        features=[
            Feature(name="fs1_my_feature_1", dtype=ValueType.INT64),
            Feature(name="fs1_my_feature_2", dtype=ValueType.STRING),
            Feature(name="fs1_my_feature_3", dtype=ValueType.STRING_LIST),
            Feature(name="fs1_my_feature_4", dtype=ValueType.BYTES_LIST),
        ],
        entities=["fs1_my_entity_1"],
        tags={"team": "matchmaking"},
        input=batch_source,
        ttl=timedelta(minutes=5),
    )

    # Register Feature View
    test_feature_store.apply([fv1])

    feature_views = test_feature_store.list_feature_views()

    # List Feature Views
    assert (
        len(feature_views) == 1
        and feature_views[0].name == "my_feature_view_1"
        and feature_views[0].features[0].name == "fs1_my_feature_1"
        and feature_views[0].features[0].dtype == ValueType.INT64
        and feature_views[0].features[1].name == "fs1_my_feature_2"
        and feature_views[0].features[1].dtype == ValueType.STRING
        and feature_views[0].features[2].name == "fs1_my_feature_3"
        and feature_views[0].features[2].dtype == ValueType.STRING_LIST
        and feature_views[0].features[3].name == "fs1_my_feature_4"
        and feature_views[0].features[3].dtype == ValueType.BYTES_LIST
        and feature_views[0].entities[0] == "fs1_my_entity_1"
    )

    feature_view = test_feature_store.get_feature_view("my_feature_view_1")
    assert (
        feature_view.name == "my_feature_view_1"
        and feature_view.features[0].name == "fs1_my_feature_1"
        and feature_view.features[0].dtype == ValueType.INT64
        and feature_view.features[1].name == "fs1_my_feature_2"
        and feature_view.features[1].dtype == ValueType.STRING
        and feature_view.features[2].name == "fs1_my_feature_3"
        and feature_view.features[2].dtype == ValueType.STRING_LIST
        and feature_view.features[3].name == "fs1_my_feature_4"
        and feature_view.features[3].dtype == ValueType.BYTES_LIST
        and feature_view.entities[0] == "fs1_my_entity_1"
    )

    test_feature_store.delete_feature_view("my_feature_view_1")
    feature_views = test_feature_store.list_feature_views()
    assert len(feature_views) == 0

    test_feature_store.teardown()


@pytest.mark.parametrize(
    "test_feature_store", [lazy_fixture("feature_store_with_local_registry")],
)
def test_apply_object_and_read(test_feature_store):
    assert isinstance(test_feature_store, FeatureStore)
    # Create Feature Views
    batch_source = FileSource(
        file_format=ParquetFormat(),
        file_url="file://feast/*",
        event_timestamp_column="ts_col",
        created_timestamp_column="timestamp",
    )

    e1 = Entity(
        name="fs1_my_entity_1", value_type=ValueType.STRING, description="something"
    )

    e2 = Entity(
        name="fs1_my_entity_2", value_type=ValueType.STRING, description="something"
    )

    fv1 = FeatureView(
        name="my_feature_view_1",
        features=[
            Feature(name="fs1_my_feature_1", dtype=ValueType.INT64),
            Feature(name="fs1_my_feature_2", dtype=ValueType.STRING),
            Feature(name="fs1_my_feature_3", dtype=ValueType.STRING_LIST),
            Feature(name="fs1_my_feature_4", dtype=ValueType.BYTES_LIST),
        ],
        entities=["fs1_my_entity_1"],
        tags={"team": "matchmaking"},
        input=batch_source,
        ttl=timedelta(minutes=5),
    )

    fv2 = FeatureView(
        name="my_feature_view_2",
        features=[
            Feature(name="fs1_my_feature_1", dtype=ValueType.INT64),
            Feature(name="fs1_my_feature_2", dtype=ValueType.STRING),
            Feature(name="fs1_my_feature_3", dtype=ValueType.STRING_LIST),
            Feature(name="fs1_my_feature_4", dtype=ValueType.BYTES_LIST),
        ],
        entities=["fs1_my_entity_1"],
        tags={"team": "matchmaking"},
        input=batch_source,
        ttl=timedelta(minutes=5),
    )

    # Register Feature View
    test_feature_store.apply([fv1, e1, fv2, e2])

    fv1_actual = test_feature_store.get_feature_view("my_feature_view_1")
    e1_actual = test_feature_store.get_entity("fs1_my_entity_1")

    assert fv1 == fv1_actual
    assert e1 == e1_actual
    assert fv2 != fv1_actual
    assert e2 != e1_actual

    test_feature_store.teardown()


def test_apply_remote_repo():
    fd, registry_path = mkstemp()
    fd, online_store_path = mkstemp()
    return FeatureStore(
        config=RepoConfig(
            registry=registry_path,
            project="default",
            provider="local",
            online_store=SqliteOnlineStoreConfig(path=online_store_path),
        )
    )


@pytest.mark.parametrize(
    "test_feature_store", [lazy_fixture("feature_store_with_local_registry")],
)
@pytest.mark.parametrize("dataframe_source", [lazy_fixture("simple_dataset_1")])
def test_reapply_feature_view_success(test_feature_store, dataframe_source):
    with prep_file_source(
        df=dataframe_source, event_timestamp_column="ts_1"
    ) as file_source:

        e = Entity(name="id", value_type=ValueType.STRING)

        # Create Feature View
        fv1 = FeatureView(
            name="my_feature_view_1",
            features=[Feature(name="string_col", dtype=ValueType.STRING)],
            entities=["id"],
            input=file_source,
            ttl=timedelta(minutes=5),
        )

        # Register Feature View
        test_feature_store.apply([fv1, e])

        # Check Feature View
        fv_stored = test_feature_store.get_feature_view(fv1.name)
        assert len(fv_stored.materialization_intervals) == 0

        # Run materialization
        test_feature_store.materialize(datetime(2020, 1, 1), datetime(2021, 1, 1))

        # Check Feature View
        fv_stored = test_feature_store.get_feature_view(fv1.name)
        assert len(fv_stored.materialization_intervals) == 1

        # Apply again
        test_feature_store.apply([fv1])

        # Check Feature View
        fv_stored = test_feature_store.get_feature_view(fv1.name)
        assert len(fv_stored.materialization_intervals) == 1

        # Change and apply Feature View
        fv1 = FeatureView(
            name="my_feature_view_1",
            features=[Feature(name="int64_col", dtype=ValueType.INT64)],
            entities=["id"],
            input=file_source,
            ttl=timedelta(minutes=5),
        )
        test_feature_store.apply([fv1])

        # Check Feature View
        fv_stored = test_feature_store.get_feature_view(fv1.name)
        assert len(fv_stored.materialization_intervals) == 0

        test_feature_store.teardown()
