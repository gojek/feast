import contextlib
import math
import tempfile
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator, Optional, Tuple, Union

import pandas as pd
import pytest
from google.cloud import bigquery
from pytz import timezone, utc

from feast.data_format import ParquetFormat
from feast.data_source import BigQuerySource, FileSource
from feast.entity import Entity
from feast.feature import Feature
from feast.feature_store import FeatureStore
from feast.feature_view import FeatureView
from feast.infra.offline_stores.file import FileOfflineStoreConfig
from feast.infra.online_stores.datastore import DatastoreOnlineStoreConfig
from feast.infra.online_stores.dynamodb import DynamoDBOnlineStoreConfig
from feast.infra.online_stores.redis import RedisOnlineStoreConfig, RedisType
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.repo_config import RepoConfig
from feast.value_type import ValueType


def create_dataset() -> pd.DataFrame:
    now = datetime.utcnow()
    ts = pd.Timestamp(now).round("ms")
    data = {
        "id": [1, 2, 1, 3, 3],
        "value": [0.1, None, 0.3, 4, 5],
        "ts_1": [
            ts - timedelta(hours=4),
            ts,
            ts - timedelta(hours=3),
            # Use different time zones to test tz-naive -> tz-aware conversion
            (ts - timedelta(hours=4))
            .replace(tzinfo=utc)
            .astimezone(tz=timezone("Europe/Berlin")),
            (ts - timedelta(hours=1))
            .replace(tzinfo=utc)
            .astimezone(tz=timezone("US/Pacific")),
        ],
        "created_ts": [ts, ts, ts, ts, ts],
    }
    return pd.DataFrame.from_dict(data)


def get_feature_view(data_source: Union[FileSource, BigQuerySource]) -> FeatureView:
    return FeatureView(
        name="test_bq_correctness",
        entities=["driver"],
        features=[Feature("value", ValueType.FLOAT)],
        ttl=timedelta(days=5),
        input=data_source,
    )


# bq_source_type must be one of "query" and "table"
@contextlib.contextmanager
def prep_bq_fs_and_fv(
    bq_source_type: str,
) -> Iterator[Tuple[FeatureStore, FeatureView]]:
    client = bigquery.Client()
    gcp_project = client.project
    bigquery_dataset = "test_ingestion"
    dataset = bigquery.Dataset(f"{gcp_project}.{bigquery_dataset}")
    client.create_dataset(dataset, exists_ok=True)
    dataset.default_table_expiration_ms = (
        1000 * 60 * 60 * 24 * 14
    )  # 2 weeks in milliseconds
    client.update_dataset(dataset, ["default_table_expiration_ms"])

    df = create_dataset()

    job_config = bigquery.LoadJobConfig()
    table_ref = f"{gcp_project}.{bigquery_dataset}.{bq_source_type}_correctness_{int(time.time_ns())}"
    query = f"SELECT * FROM `{table_ref}`"
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    bigquery_source = BigQuerySource(
        table_ref=table_ref if bq_source_type == "table" else None,
        query=query if bq_source_type == "query" else None,
        event_timestamp_column="ts",
        created_timestamp_column="created_ts",
        date_partition_column="",
        field_mapping={"ts_1": "ts", "id": "driver_id"},
    )

    fv = get_feature_view(bigquery_source)
    e = Entity(
        name="driver",
        description="id for driver",
        join_key="driver_id",
        value_type=ValueType.INT32,
    )
    with tempfile.TemporaryDirectory() as repo_dir_name:
        config = RepoConfig(
            registry=str(Path(repo_dir_name) / "registry.db"),
            project=f"test_bq_correctness_{str(uuid.uuid4()).replace('-', '')}",
            provider="gcp",
            online_store=DatastoreOnlineStoreConfig(namespace="integration_test"),
        )
        fs = FeatureStore(config=config)
        fs.apply([fv, e])

        yield fs, fv

        fs.teardown()


@contextlib.contextmanager
def prep_local_fs_and_fv() -> Iterator[Tuple[FeatureStore, FeatureView]]:
    with tempfile.NamedTemporaryFile(suffix=".parquet") as f:
        df = create_dataset()
        f.close()
        df.to_parquet(f.name)
        file_source = FileSource(
            file_format=ParquetFormat(),
            file_url=f"file://{f.name}",
            event_timestamp_column="ts",
            created_timestamp_column="created_ts",
            date_partition_column="",
            field_mapping={"ts_1": "ts", "id": "driver_id"},
        )
        fv = get_feature_view(file_source)
        e = Entity(
            name="driver",
            description="id for driver",
            join_key="driver_id",
            value_type=ValueType.INT32,
        )
        project = f"test_local_correctness_{str(uuid.uuid4()).replace('-', '')}"
        print(f"Using project: {project}")

        with tempfile.TemporaryDirectory() as repo_dir_name, tempfile.TemporaryDirectory() as data_dir_name:
            config = RepoConfig(
                registry=str(Path(repo_dir_name) / "registry.db"),
                project=project,
                provider="local",
                online_store=SqliteOnlineStoreConfig(
                    path=str(Path(data_dir_name) / "online_store.db")
                ),
            )
            fs = FeatureStore(config=config)
            fs.apply([fv, e])

            yield fs, fv

            fs.teardown()


@contextlib.contextmanager
def prep_redis_fs_and_fv() -> Iterator[Tuple[FeatureStore, FeatureView]]:
    with tempfile.NamedTemporaryFile(suffix=".parquet") as f:
        df = create_dataset()
        f.close()
        df.to_parquet(f.name)
        file_source = FileSource(
            file_format=ParquetFormat(),
            file_url=f"file://{f.name}",
            event_timestamp_column="ts",
            created_timestamp_column="created_ts",
            date_partition_column="",
            field_mapping={"ts_1": "ts", "id": "driver_id"},
        )
        fv = get_feature_view(file_source)
        e = Entity(
            name="driver",
            description="id for driver",
            join_key="driver_id",
            value_type=ValueType.INT32,
        )
        project = f"test_redis_correctness_{str(uuid.uuid4()).replace('-', '')}"
        print(f"Using project: {project}")
        with tempfile.TemporaryDirectory() as repo_dir_name:
            config = RepoConfig(
                registry=str(Path(repo_dir_name) / "registry.db"),
                project=project,
                provider="local",
                online_store=RedisOnlineStoreConfig(
                    type="redis",
                    redis_type=RedisType.redis,
                    connection_string="localhost:6379,db=0",
                ),
            )
            fs = FeatureStore(config=config)
            fs.apply([fv, e])

            yield fs, fv

            fs.teardown()


@contextlib.contextmanager
def prep_dynamodb_fs_and_fv() -> Iterator[Tuple[FeatureStore, FeatureView]]:
    with tempfile.NamedTemporaryFile(suffix=".parquet") as f:
        df = create_dataset()
        f.close()
        df.to_parquet(f.name)
        file_source = FileSource(
            file_format=ParquetFormat(),
            file_url=f"file://{f.name}",
            event_timestamp_column="ts",
            created_timestamp_column="created_ts",
            date_partition_column="",
            field_mapping={"ts_1": "ts", "id": "driver_id"},
        )
        fv = get_feature_view(file_source)
        e = Entity(
            name="driver",
            description="id for driver",
            join_key="driver_id",
            value_type=ValueType.INT32,
        )
        project = f"test_dynamo_correctness_{str(uuid.uuid4()).replace('-', '')}"
        print(f"Using project {project}")
        with tempfile.TemporaryDirectory() as repo_dir_name:
            config = RepoConfig(
                registry=str(Path(repo_dir_name) / "registry.db"),
                project=project,
                provider="aws",
                online_store=DynamoDBOnlineStoreConfig(region="us-west-2"),
                offline_store=FileOfflineStoreConfig(),
            )
            fs = FeatureStore(config=config)
            fs.apply([fv, e])

            yield fs, fv

            fs.teardown()


# Checks that both offline & online store values are as expected
def check_offline_and_online_features(
    fs: FeatureStore,
    fv: FeatureView,
    driver_id: int,
    event_timestamp: datetime,
    expected_value: Optional[float],
    full_feature_names: bool,
) -> None:
    # Check online store
    response_dict = fs.get_online_features(
        [f"{fv.name}:value"],
        [{"driver": driver_id}],
        full_feature_names=full_feature_names,
    ).to_dict()

    if full_feature_names:
        if expected_value:
            assert abs(response_dict[f"{fv.name}__value"][0] - expected_value) < 1e-6
        else:
            assert response_dict[f"{fv.name}__value"][0] is None
    else:
        if expected_value:
            assert abs(response_dict["value"][0] - expected_value) < 1e-6
        else:
            assert response_dict["value"][0] is None

    # Check offline store
    df = fs.get_historical_features(
        entity_df=pd.DataFrame.from_dict(
            {"driver_id": [driver_id], "event_timestamp": [event_timestamp]}
        ),
        features=[f"{fv.name}:value"],
        full_feature_names=full_feature_names,
    ).to_df()

    if full_feature_names:
        if expected_value:
            assert abs(df.to_dict()[f"{fv.name}__value"][0] - expected_value) < 1e-6
        else:
            assert math.isnan(df.to_dict()[f"{fv.name}__value"][0])
    else:
        if expected_value:
            assert abs(df.to_dict()["value"][0] - expected_value) < 1e-6
        else:
            assert math.isnan(df.to_dict()["value"][0])


def run_offline_online_store_consistency_test(
    fs: FeatureStore, fv: FeatureView, full_feature_names: bool
) -> None:
    now = datetime.utcnow()
    # Run materialize()
    # use both tz-naive & tz-aware timestamps to test that they're both correctly handled
    start_date = (now - timedelta(hours=5)).replace(tzinfo=utc)
    end_date = now - timedelta(hours=2)
    fs.materialize(feature_views=[fv.name], start_date=start_date, end_date=end_date)

    # check result of materialize()
    check_offline_and_online_features(
        fs=fs,
        fv=fv,
        driver_id=1,
        event_timestamp=end_date,
        expected_value=0.3,
        full_feature_names=full_feature_names,
    )

    check_offline_and_online_features(
        fs=fs,
        fv=fv,
        driver_id=2,
        event_timestamp=end_date,
        expected_value=None,
        full_feature_names=full_feature_names,
    )

    # check prior value for materialize_incremental()
    check_offline_and_online_features(
        fs=fs,
        fv=fv,
        driver_id=3,
        event_timestamp=end_date,
        expected_value=4,
        full_feature_names=full_feature_names,
    )

    # run materialize_incremental()
    fs.materialize_incremental(feature_views=[fv.name], end_date=now)

    # check result of materialize_incremental()
    check_offline_and_online_features(
        fs=fs,
        fv=fv,
        driver_id=3,
        event_timestamp=now,
        expected_value=5,
        full_feature_names=full_feature_names,
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    "bq_source_type", ["query", "table"],
)
@pytest.mark.parametrize("full_feature_names", [True, False])
def test_bq_offline_online_store_consistency(
    bq_source_type: str, full_feature_names: bool
):
    with prep_bq_fs_and_fv(bq_source_type) as (fs, fv):
        run_offline_online_store_consistency_test(fs, fv, full_feature_names)


@pytest.mark.parametrize("full_feature_names", [True, False])
@pytest.mark.integration
def test_redis_offline_online_store_consistency(full_feature_names: bool):
    with prep_redis_fs_and_fv() as (fs, fv):
        run_offline_online_store_consistency_test(fs, fv, full_feature_names)


@pytest.mark.parametrize("full_feature_names", [True, False])
@pytest.mark.integration
def test_dynamodb_offline_online_store_consistency(full_feature_names: bool):
    with prep_dynamodb_fs_and_fv() as (fs, fv):
        run_offline_online_store_consistency_test(fs, fv, full_feature_names)


@pytest.mark.parametrize("full_feature_names", [True, False])
def test_local_offline_online_store_consistency(full_feature_names: bool):
    with prep_local_fs_and_fv() as (fs, fv):
        run_offline_online_store_consistency_test(fs, fv, full_feature_names)
