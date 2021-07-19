# Copyright 2021 The Feast Authors
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
from datetime import timedelta
from tempfile import mkstemp

import pytest
from pytest_lazyfixture import lazy_fixture

from feast import FileSource
from feast.data_format import ParquetFormat
from feast.entity import Entity
from feast.feature import Feature
from feast.feature_view import FeatureView
from feast.protos.feast.types import Value_pb2 as ValueProto
from feast.registry import Registry
from feast.value_type import ValueType


@pytest.fixture
def local_registry():
    fd, registry_path = mkstemp()
    return Registry(registry_path, None, timedelta(600))


@pytest.fixture
def gcs_registry():
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
    return Registry(f"gs://{bucket_name}/registry.db", None, timedelta(600))


@pytest.fixture
def s3_registry():
    return Registry(
        f"s3://feast-integration-tests/registries/{int(time.time() * 1000)}/registry.db",
        None,
        timedelta(600),
    )


@pytest.mark.parametrize(
    "test_registry", [lazy_fixture("local_registry")],
)
def test_apply_entity_success(test_registry):
    entity = Entity(
        name="driver_car_id",
        description="Car driver id",
        value_type=ValueType.STRING,
        labels={"team": "matchmaking"},
    )

    project = "project"

    # Register Entity
    test_registry.apply_entity(entity, project)

    entities = test_registry.list_entities(project)

    entity = entities[0]
    assert (
        len(entities) == 1
        and entity.name == "driver_car_id"
        and entity.value_type == ValueType(ValueProto.ValueType.STRING)
        and entity.description == "Car driver id"
        and "team" in entity.labels
        and entity.labels["team"] == "matchmaking"
    )

    entity = test_registry.get_entity("driver_car_id", project)
    assert (
        entity.name == "driver_car_id"
        and entity.value_type == ValueType(ValueProto.ValueType.STRING)
        and entity.description == "Car driver id"
        and "team" in entity.labels
        and entity.labels["team"] == "matchmaking"
    )

    test_registry.teardown()

    # Will try to reload registry, which will fail because the file has been deleted
    with pytest.raises(FileNotFoundError):
        test_registry._get_registry_proto()


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_registry", [lazy_fixture("gcs_registry"), lazy_fixture("s3_registry")],
)
def test_apply_entity_integration(test_registry):
    entity = Entity(
        name="driver_car_id",
        description="Car driver id",
        value_type=ValueType.STRING,
        labels={"team": "matchmaking"},
    )

    project = "project"

    # Register Entity
    test_registry.apply_entity(entity, project)

    entities = test_registry.list_entities(project)

    entity = entities[0]
    assert (
        len(entities) == 1
        and entity.name == "driver_car_id"
        and entity.value_type == ValueType(ValueProto.ValueType.STRING)
        and entity.description == "Car driver id"
        and "team" in entity.labels
        and entity.labels["team"] == "matchmaking"
    )

    entity = test_registry.get_entity("driver_car_id", project)
    assert (
        entity.name == "driver_car_id"
        and entity.value_type == ValueType(ValueProto.ValueType.STRING)
        and entity.description == "Car driver id"
        and "team" in entity.labels
        and entity.labels["team"] == "matchmaking"
    )

    test_registry.teardown()

    # Will try to reload registry, which will fail because the file has been deleted
    with pytest.raises(FileNotFoundError):
        test_registry._get_registry_proto()


@pytest.mark.parametrize(
    "test_registry", [lazy_fixture("local_registry")],
)
def test_apply_feature_view_success(test_registry):
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

    project = "project"

    # Register Feature View
    test_registry.apply_feature_view(fv1, project)

    feature_views = test_registry.list_feature_views(project)

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

    feature_view = test_registry.get_feature_view("my_feature_view_1", project)
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

    test_registry.delete_feature_view("my_feature_view_1", project)
    feature_views = test_registry.list_feature_views(project)
    assert len(feature_views) == 0

    test_registry.teardown()

    # Will try to reload registry, which will fail because the file has been deleted
    with pytest.raises(FileNotFoundError):
        test_registry._get_registry_proto()


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_registry", [lazy_fixture("gcs_registry"), lazy_fixture("s3_registry")],
)
def test_apply_feature_view_integration(test_registry):
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

    project = "project"

    # Register Feature View
    test_registry.apply_feature_view(fv1, project)

    feature_views = test_registry.list_feature_views(project)

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

    feature_view = test_registry.get_feature_view("my_feature_view_1", project)
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

    test_registry.delete_feature_view("my_feature_view_1", project)
    feature_views = test_registry.list_feature_views(project)
    assert len(feature_views) == 0

    test_registry.teardown()

    # Will try to reload registry, which will fail because the file has been deleted
    with pytest.raises(FileNotFoundError):
        test_registry._get_registry_proto()


def test_commit():
    fd, registry_path = mkstemp()
    test_registry = Registry(registry_path, None, timedelta(600))

    entity = Entity(
        name="driver_car_id",
        description="Car driver id",
        value_type=ValueType.STRING,
        labels={"team": "matchmaking"},
    )

    project = "project"

    # Register Entity without commiting
    test_registry.apply_entity(entity, project, commit=False)

    # Retrieving the entity should still succeed
    entities = test_registry.list_entities(project, allow_cache=True)

    entity = entities[0]
    assert (
        len(entities) == 1
        and entity.name == "driver_car_id"
        and entity.value_type == ValueType(ValueProto.ValueType.STRING)
        and entity.description == "Car driver id"
        and "team" in entity.labels
        and entity.labels["team"] == "matchmaking"
    )

    entity = test_registry.get_entity("driver_car_id", project, allow_cache=True)
    assert (
        entity.name == "driver_car_id"
        and entity.value_type == ValueType(ValueProto.ValueType.STRING)
        and entity.description == "Car driver id"
        and "team" in entity.labels
        and entity.labels["team"] == "matchmaking"
    )

    # Create new registry that points to the same store
    registry_with_same_store = Registry(registry_path, None, timedelta(600))

    # Retrieving the entity should fail since the store is empty
    entities = registry_with_same_store.list_entities(project)
    assert len(entities) == 0

    # commit from the original registry
    test_registry.commit()

    # Reconstruct the new registry in order to read the newly written store
    registry_with_same_store = Registry(registry_path, None, timedelta(600))

    # Retrieving the entity should now succeed
    entities = registry_with_same_store.list_entities(project)

    entity = entities[0]
    assert (
        len(entities) == 1
        and entity.name == "driver_car_id"
        and entity.value_type == ValueType(ValueProto.ValueType.STRING)
        and entity.description == "Car driver id"
        and "team" in entity.labels
        and entity.labels["team"] == "matchmaking"
    )

    entity = test_registry.get_entity("driver_car_id", project)
    assert (
        entity.name == "driver_car_id"
        and entity.value_type == ValueType(ValueProto.ValueType.STRING)
        and entity.description == "Car driver id"
        and "team" in entity.labels
        and entity.labels["team"] == "matchmaking"
    )

    test_registry.teardown()

    # Will try to reload registry, which will fail because the file has been deleted
    with pytest.raises(FileNotFoundError):
        test_registry._get_registry_proto()
