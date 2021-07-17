import os
import random
import string
import time
from datetime import datetime, timedelta
from tempfile import TemporaryDirectory

import assertpy
import numpy as np
import pandas as pd
import pytest
from google.cloud import bigquery
from pandas.testing import assert_frame_equal
from pytz import utc

import feast.driver_test_data as driver_data
from feast import RepoConfig, errors, utils
from feast.data_source import BigQuerySource, FileSource
from feast.entity import Entity
from feast.errors import FeatureNameCollisionError
from feast.feature import Feature
from feast.feature_store import FeatureStore, _validate_feature_refs
from feast.feature_view import FeatureView
from feast.infra.offline_stores.bigquery import (
    BigQueryOfflineStoreConfig,
    _get_entity_df_timestamp_bounds,
)
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.infra.provider import DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
from feast.value_type import ValueType

np.random.seed(0)

PROJECT_NAME = "default"


def generate_entities(date, infer_event_timestamp_col, order_count: int = 1000):
    end_date = date
    before_start_date = end_date - timedelta(days=365)
    start_date = end_date - timedelta(days=7)
    after_end_date = end_date + timedelta(days=365)
    customer_entities = list(range(1001, 1110))
    driver_entities = list(range(5001, 5110))
    orders_df = driver_data.create_orders_df(
        customers=customer_entities,
        drivers=driver_entities,
        start_date=before_start_date,
        end_date=after_end_date,
        order_count=order_count,
        infer_event_timestamp_col=infer_event_timestamp_col,
    )
    return customer_entities, driver_entities, end_date, orders_df, start_date


def stage_driver_hourly_stats_parquet_source(directory, df):
    # Write to disk
    driver_stats_path = os.path.join(directory, "driver_stats.parquet")
    df.to_parquet(path=driver_stats_path, allow_truncated_timestamps=True)
    return FileSource(
        path=driver_stats_path,
        event_timestamp_column="datetime",
        created_timestamp_column="",
    )


def stage_driver_hourly_stats_bigquery_source(df, table_id):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()
    df.reset_index(drop=True, inplace=True)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


def create_driver_hourly_stats_feature_view(source):
    driver_stats_feature_view = FeatureView(
        name="driver_stats",
        entities=["driver"],
        features=[
            Feature(name="conv_rate", dtype=ValueType.FLOAT),
            Feature(name="acc_rate", dtype=ValueType.FLOAT),
            Feature(name="avg_daily_trips", dtype=ValueType.INT32),
        ],
        input=source,
        ttl=timedelta(hours=2),
    )
    return driver_stats_feature_view


def stage_customer_daily_profile_parquet_source(directory, df):
    customer_profile_path = os.path.join(directory, "customer_profile.parquet")
    df.to_parquet(path=customer_profile_path, allow_truncated_timestamps=True)
    return FileSource(
        path=customer_profile_path,
        event_timestamp_column="datetime",
        created_timestamp_column="created",
    )


def stage_customer_daily_profile_bigquery_source(df, table_id):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()
    df.reset_index(drop=True, inplace=True)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


def create_customer_daily_profile_feature_view(source):
    customer_profile_feature_view = FeatureView(
        name="customer_profile",
        entities=["customer_id"],
        features=[
            Feature(name="current_balance", dtype=ValueType.FLOAT),
            Feature(name="avg_passenger_count", dtype=ValueType.FLOAT),
            Feature(name="lifetime_trip_count", dtype=ValueType.INT32),
            Feature(name="avg_daily_trips", dtype=ValueType.INT32),
        ],
        input=source,
        ttl=timedelta(days=2),
    )
    return customer_profile_feature_view


# Converts the given column of the pandas records to UTC timestamps
def convert_timestamp_records_to_utc(records, column):
    for record in records:
        record[column] = utils.make_tzaware(record[column]).astimezone(utc)
    return records


# Find the latest record in the given time range and filter
def find_asof_record(records, ts_key, ts_start, ts_end, filter_key, filter_value):
    found_record = {}
    for record in records:
        if record[filter_key] == filter_value and ts_start <= record[ts_key] <= ts_end:
            if not found_record or found_record[ts_key] < record[ts_key]:
                found_record = record
    return found_record


def get_expected_training_df(
    customer_df: pd.DataFrame,
    customer_fv: FeatureView,
    driver_df: pd.DataFrame,
    driver_fv: FeatureView,
    orders_df: pd.DataFrame,
    event_timestamp: str,
    full_feature_names: bool = False,
):
    # Convert all pandas dataframes into records with UTC timestamps
    order_records = convert_timestamp_records_to_utc(
        orders_df.to_dict("records"), event_timestamp
    )
    driver_records = convert_timestamp_records_to_utc(
        driver_df.to_dict("records"), driver_fv.input.event_timestamp_column
    )
    customer_records = convert_timestamp_records_to_utc(
        customer_df.to_dict("records"), customer_fv.input.event_timestamp_column
    )

    # Manually do point-in-time join of orders to drivers and customers records
    for order_record in order_records:
        driver_record = find_asof_record(
            driver_records,
            ts_key=driver_fv.input.event_timestamp_column,
            ts_start=order_record[event_timestamp] - driver_fv.ttl,
            ts_end=order_record[event_timestamp],
            filter_key="driver_id",
            filter_value=order_record["driver_id"],
        )
        customer_record = find_asof_record(
            customer_records,
            ts_key=customer_fv.input.event_timestamp_column,
            ts_start=order_record[event_timestamp] - customer_fv.ttl,
            ts_end=order_record[event_timestamp],
            filter_key="customer_id",
            filter_value=order_record["customer_id"],
        )

        order_record.update(
            {
                (f"driver_stats__{k}" if full_feature_names else k): driver_record.get(
                    k, None
                )
                for k in ("conv_rate", "avg_daily_trips")
            }
        )

        order_record.update(
            {
                (
                    f"customer_profile__{k}" if full_feature_names else k
                ): customer_record.get(k, None)
                for k in (
                    "current_balance",
                    "avg_passenger_count",
                    "lifetime_trip_count",
                )
            }
        )

    # Convert records back to pandas dataframe
    expected_df = pd.DataFrame(order_records)

    # Move "datetime" column to front
    current_cols = expected_df.columns.tolist()
    current_cols.remove(event_timestamp)
    expected_df = expected_df[[event_timestamp] + current_cols]

    # Cast some columns to expected types, since we lose information when converting pandas DFs into Python objects.
    if full_feature_names:
        expected_column_types = {
            "order_is_success": "int32",
            "driver_stats__conv_rate": "float32",
            "customer_profile__current_balance": "float32",
            "customer_profile__avg_passenger_count": "float32",
        }
    else:
        expected_column_types = {
            "order_is_success": "int32",
            "conv_rate": "float32",
            "current_balance": "float32",
            "avg_passenger_count": "float32",
        }

    for col, typ in expected_column_types.items():
        expected_df[col] = expected_df[col].astype(typ)

    return expected_df


def stage_orders_bigquery(df, table_id):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()
    df.reset_index(drop=True, inplace=True)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


class BigQueryDataSet:
    def __init__(self, dataset_name):
        self.name = dataset_name

    def __enter__(self):
        client = bigquery.Client()
        dataset = bigquery.Dataset(f"{client.project}.{self.name}")
        dataset.location = "US"
        dataset = client.create_dataset(dataset, exists_ok=True)
        return dataset

    def __exit__(self, exc_type, exc_value, exc_traceback):
        print("Tearing down BigQuery dataset")
        client = bigquery.Client()
        dataset_id = f"{client.project}.{self.name}"

        client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
        print(f"Deleted dataset '{dataset_id}'")
        if exc_type:
            print(
                "***Logging exception {}***".format(
                    (exc_type, exc_value, exc_traceback)
                )
            )


@pytest.mark.parametrize(
    "infer_event_timestamp_col", [False, True],
)
@pytest.mark.parametrize(
    "full_feature_names", [False, True],
)
def test_historical_features_from_parquet_sources(
    infer_event_timestamp_col, full_feature_names
):
    start_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    (
        customer_entities,
        driver_entities,
        end_date,
        orders_df,
        start_date,
    ) = generate_entities(start_date, infer_event_timestamp_col)

    with TemporaryDirectory() as temp_dir:
        driver_df = driver_data.create_driver_hourly_stats_df(
            driver_entities, start_date, end_date
        )
        driver_source = stage_driver_hourly_stats_parquet_source(temp_dir, driver_df)
        driver_fv = create_driver_hourly_stats_feature_view(driver_source)
        customer_df = driver_data.create_customer_daily_profile_df(
            customer_entities, start_date, end_date
        )
        customer_source = stage_customer_daily_profile_parquet_source(
            temp_dir, customer_df
        )
        customer_fv = create_customer_daily_profile_feature_view(customer_source)
        driver = Entity(name="driver", join_key="driver_id", value_type=ValueType.INT64)
        customer = Entity(name="customer_id", value_type=ValueType.INT64)

        store = FeatureStore(
            config=RepoConfig(
                registry=os.path.join(temp_dir, "registry.db"),
                project="default",
                provider="local",
                online_store=SqliteOnlineStoreConfig(
                    path=os.path.join(temp_dir, "online_store.db")
                ),
            )
        )

        store.apply([driver, customer, driver_fv, customer_fv])

        job = store.get_historical_features(
            entity_df=orders_df,
            feature_refs=[
                "driver_stats:conv_rate",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
            ],
            full_feature_names=full_feature_names,
        )

        actual_df = job.to_df()
        event_timestamp = (
            DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
            if DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL in orders_df.columns
            else "e_ts"
        )
        expected_df = get_expected_training_df(
            customer_df,
            customer_fv,
            driver_df,
            driver_fv,
            orders_df,
            event_timestamp,
            full_feature_names=full_feature_names,
        )
        assert_frame_equal(
            expected_df.sort_values(
                by=[event_timestamp, "order_id", "driver_id", "customer_id"]
            ).reset_index(drop=True),
            actual_df.sort_values(
                by=[event_timestamp, "order_id", "driver_id", "customer_id"]
            ).reset_index(drop=True),
        )


@pytest.mark.parametrize(
    "entity_df,entity_id,feature_refs",
    [
        (
            "driver_stats",
            "driver_id",
            ["driver_stats:conv_rate", "driver_stats:avg_daily_trips"],
        ),
        (
            "customer_profile",
            "customer_id",
            [
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
            ],
        ),
    ],
)
@pytest.mark.parametrize(
    "entity_start_date,entity_end_date",
    [(None, None), (timedelta(days=-7), timedelta(days=-1))],
)
def test_historical_features_from_parquet_sources_infer_entity_df(
    entity_df, entity_id, feature_refs, entity_start_date, entity_end_date
):
    start_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    (customer_entities, driver_entities, end_date, _, start_date,) = generate_entities(
        start_date, True
    )

    with TemporaryDirectory() as temp_dir:
        driver_df = driver_data.create_driver_hourly_stats_df(
            driver_entities, start_date, end_date
        )
        driver_source = stage_driver_hourly_stats_parquet_source(temp_dir, driver_df)
        driver_fv = create_driver_hourly_stats_feature_view(driver_source)
        customer_df = driver_data.create_customer_daily_profile_df(
            customer_entities, start_date, end_date
        )
        customer_source = stage_customer_daily_profile_parquet_source(
            temp_dir, customer_df
        )
        customer_fv = create_customer_daily_profile_feature_view(customer_source)
        driver = Entity(name="driver", join_key="driver_id", value_type=ValueType.INT64)
        customer = Entity(name="customer_id", value_type=ValueType.INT64)

        start_date = (
            start_date + entity_start_date if entity_start_date is not None else None
        )
        end_date = end_date + entity_end_date if entity_end_date is not None else None

        store = FeatureStore(
            config=RepoConfig(
                registry=os.path.join(temp_dir, "registry.db"),
                project="default",
                provider="local",
                online_store=SqliteOnlineStoreConfig(
                    path=os.path.join(temp_dir, "online_store.db")
                ),
            )
        )

        store.apply([driver, customer, driver_fv, customer_fv])

        job_implicit = store.get_historical_features_by_view(
            entity_df=entity_df,
            feature_refs=feature_refs,
            start_date=start_date,
            end_date=end_date,
        )

        target_df = driver_df if entity_df == "driver_stats" else customer_df
        target_df = target_df[[entity_id, "datetime"]].rename(
            columns={"datetime": DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL}
        )
        target_df[DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL] = pd.to_datetime(
            target_df[DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL], utc=True
        )
        if not (start_date is None and end_date is None):
            target_df = target_df[
                (
                    target_df[DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL]
                    >= pd.to_datetime(start_date, utc=True)
                )
                & (
                    target_df[DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL]
                    < pd.to_datetime(end_date, utc=True)
                )
            ]
        job_explicit = store.get_historical_features(
            entity_df=target_df, feature_refs=feature_refs,
        )

        implicit_df = job_implicit.to_df()
        explicit_df = job_explicit.to_df()

        assert_frame_equal(
            implicit_df.sort_values(
                by=[DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL, entity_id]
            ).reset_index(drop=True),
            explicit_df.sort_values(
                by=[DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL, entity_id]
            ).reset_index(drop=True),
        )


@pytest.mark.integration
@pytest.mark.parametrize(
    "provider_type", ["local", "gcp", "gcp_custom_offline_config"],
)
@pytest.mark.parametrize(
    "infer_event_timestamp_col", [False, True],
)
@pytest.mark.parametrize(
    "full_feature_names", [False, True],
)
def test_historical_features_from_bigquery_sources(
    provider_type, infer_event_timestamp_col, capsys, full_feature_names
):
    start_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    (
        customer_entities,
        driver_entities,
        end_date,
        orders_df,
        start_date,
    ) = generate_entities(start_date, infer_event_timestamp_col)

    bigquery_dataset = (
        f"test_hist_retrieval_{int(time.time_ns())}_{random.randint(1000, 9999)}"
    )

    with BigQueryDataSet(bigquery_dataset), TemporaryDirectory() as temp_dir:
        gcp_project = bigquery.Client().project

        # Orders Query
        table_id = f"{bigquery_dataset}.orders"
        stage_orders_bigquery(orders_df, table_id)
        entity_df_query = f"SELECT * FROM {gcp_project}.{table_id}"

        # Driver Feature View
        driver_df = driver_data.create_driver_hourly_stats_df(
            driver_entities, start_date, end_date
        )
        driver_table_id = f"{gcp_project}.{bigquery_dataset}.driver_hourly"
        stage_driver_hourly_stats_bigquery_source(driver_df, driver_table_id)
        driver_source = BigQuerySource(
            table_ref=driver_table_id,
            event_timestamp_column="datetime",
            created_timestamp_column="created",
        )
        driver_fv = create_driver_hourly_stats_feature_view(driver_source)

        # Customer Feature View
        customer_df = driver_data.create_customer_daily_profile_df(
            customer_entities, start_date, end_date
        )
        customer_table_id = f"{gcp_project}.{bigquery_dataset}.customer_profile"

        stage_customer_daily_profile_bigquery_source(customer_df, customer_table_id)
        customer_source = BigQuerySource(
            table_ref=customer_table_id,
            event_timestamp_column="datetime",
            created_timestamp_column="",
        )
        customer_fv = create_customer_daily_profile_feature_view(customer_source)

        driver = Entity(name="driver", join_key="driver_id", value_type=ValueType.INT64)
        customer = Entity(name="customer_id", value_type=ValueType.INT64)

        if provider_type == "local":
            store = FeatureStore(
                config=RepoConfig(
                    registry=os.path.join(temp_dir, "registry.db"),
                    project="default",
                    provider="local",
                    online_store=SqliteOnlineStoreConfig(
                        path=os.path.join(temp_dir, "online_store.db"),
                    ),
                    offline_store=BigQueryOfflineStoreConfig(
                        type="bigquery", dataset=bigquery_dataset
                    ),
                )
            )
        elif provider_type == "gcp":
            store = FeatureStore(
                config=RepoConfig(
                    registry=os.path.join(temp_dir, "registry.db"),
                    project="".join(
                        random.choices(string.ascii_uppercase + string.digits, k=10)
                    ),
                    provider="gcp",
                    offline_store=BigQueryOfflineStoreConfig(
                        type="bigquery", dataset=bigquery_dataset
                    ),
                )
            )
        elif provider_type == "gcp_custom_offline_config":
            store = FeatureStore(
                config=RepoConfig(
                    registry=os.path.join(temp_dir, "registry.db"),
                    project="".join(
                        random.choices(string.ascii_uppercase + string.digits, k=10)
                    ),
                    provider="gcp",
                    offline_store=BigQueryOfflineStoreConfig(
                        type="bigquery", dataset="foo"
                    ),
                )
            )
        else:
            raise Exception("Invalid provider used as part of test configuration")

        store.apply([driver, customer, driver_fv, customer_fv])

        event_timestamp = (
            DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
            if DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL in orders_df.columns
            else "e_ts"
        )
        expected_df = get_expected_training_df(
            customer_df,
            customer_fv,
            driver_df,
            driver_fv,
            orders_df,
            event_timestamp,
            full_feature_names,
        )

        job_from_sql = store.get_historical_features(
            entity_df=entity_df_query,
            feature_refs=[
                "driver_stats:conv_rate",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
            ],
            full_feature_names=full_feature_names,
        )

        start_time = datetime.utcnow()
        actual_df_from_sql_entities = job_from_sql.to_df()
        end_time = datetime.utcnow()
        with capsys.disabled():
            print(
                str(
                    f"\nTime to execute job_from_sql.to_df() = '{(end_time - start_time)}'"
                )
            )

        assert sorted(expected_df.columns) == sorted(
            actual_df_from_sql_entities.columns
        )
        assert_frame_equal(
            expected_df.sort_values(
                by=[event_timestamp, "order_id", "driver_id", "customer_id"]
            ).reset_index(drop=True),
            actual_df_from_sql_entities[expected_df.columns]
            .sort_values(by=[event_timestamp, "order_id", "driver_id", "customer_id"])
            .reset_index(drop=True),
            check_dtype=False,
        )

        table_from_sql_entities = job_from_sql.to_arrow()
        assert_frame_equal(
            actual_df_from_sql_entities, table_from_sql_entities.to_pandas()
        )

        timestamp_column = (
            "e_ts"
            if infer_event_timestamp_col
            else DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
        )

        entity_df_query_with_invalid_join_key = (
            f"select order_id, driver_id, customer_id as customer, "
            f"order_is_success, {timestamp_column}, FROM {gcp_project}.{table_id}"
        )
        # Rename the join key; this should now raise an error.
        assertpy.assert_that(store.get_historical_features).raises(
            errors.FeastEntityDFMissingColumnsError
        ).when_called_with(
            entity_df=entity_df_query_with_invalid_join_key,
            feature_refs=[
                "driver_stats:conv_rate",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
            ],
        )

        job_from_df = store.get_historical_features(
            entity_df=orders_df,
            feature_refs=[
                "driver_stats:conv_rate",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
            ],
            full_feature_names=full_feature_names,
        )

        # Rename the join key; this should now raise an error.
        orders_df_with_invalid_join_key = orders_df.rename(
            {"customer_id": "customer"}, axis="columns"
        )
        assertpy.assert_that(store.get_historical_features).raises(
            errors.FeastEntityDFMissingColumnsError
        ).when_called_with(
            entity_df=orders_df_with_invalid_join_key,
            feature_refs=[
                "driver_stats:conv_rate",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
            ],
        )

        # Make sure that custom dataset name is being used from the offline_store config
        if provider_type == "gcp_custom_offline_config":
            assertpy.assert_that(job_from_df.query).contains("foo.entity_df")
        else:
            assertpy.assert_that(job_from_df.query).contains(
                f"{bigquery_dataset}.entity_df"
            )

        start_time = datetime.utcnow()
        actual_df_from_df_entities = job_from_df.to_df()
        end_time = datetime.utcnow()
        with capsys.disabled():
            print(
                str(
                    f"Time to execute job_from_df.to_df() = '{(end_time - start_time)}'\n"
                )
            )

        assert sorted(expected_df.columns) == sorted(actual_df_from_df_entities.columns)
        assert_frame_equal(
            expected_df.sort_values(
                by=[event_timestamp, "order_id", "driver_id", "customer_id"]
            ).reset_index(drop=True),
            actual_df_from_df_entities[expected_df.columns]
            .sort_values(by=[event_timestamp, "order_id", "driver_id", "customer_id"])
            .reset_index(drop=True),
            check_dtype=False,
        )

        table_from_df_entities = job_from_df.to_arrow()
        assert_frame_equal(
            actual_df_from_df_entities, table_from_df_entities.to_pandas()
        )


@pytest.mark.integration
def test_timestamp_bound_inference_from_entity_df_using_bigquery():
    start_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    (_, _, _, entity_df, start_date) = generate_entities(
        start_date, infer_event_timestamp_col=True
    )

    table_id = f"foo.table_id_{int(time.time_ns())}_{random.randint(1000, 9999)}"
    stage_orders_bigquery(entity_df, table_id)

    client = bigquery.Client()
    table = client.get_table(table=table_id)

    # Ensure that the table expires after some time
    table.expires = datetime.utcnow() + timedelta(minutes=30)
    client.update_table(table, ["expires"])

    min_timestamp, max_timestamp = _get_entity_df_timestamp_bounds(
        client, str(table.reference), "e_ts"
    )

    assert min_timestamp.astimezone("UTC") == min(entity_df["e_ts"]).astimezone("UTC")
    assert max_timestamp.astimezone("UTC") == max(entity_df["e_ts"]).astimezone("UTC")


def test_feature_name_collision_on_historical_retrieval():

    # _validate_feature_refs is the function that checks for colliding feature names
    # check when feature names collide and 'full_feature_names=False'
    with pytest.raises(FeatureNameCollisionError) as error:
        _validate_feature_refs(
            feature_refs=[
                "driver_stats:conv_rate",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
                "customer_profile:avg_daily_trips",
            ],
            full_feature_names=False,
        )

    expected_error_message = (
        "Duplicate features named avg_daily_trips found.\n"
        "To resolve this collision, either use the full feature name by setting "
        "'full_feature_names=True', or ensure that the features in question have different names."
    )

    assert str(error.value) == expected_error_message

    # check when feature names collide and 'full_feature_names=True'
    with pytest.raises(FeatureNameCollisionError) as error:
        _validate_feature_refs(
            feature_refs=[
                "driver_stats:conv_rate",
                "driver_stats:avg_daily_trips",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
                "customer_profile:avg_daily_trips",
            ],
            full_feature_names=True,
        )

    expected_error_message = (
        "Duplicate features named driver_stats__avg_daily_trips found.\n"
        "To resolve this collision, please ensure that the features in question "
        "have different names."
    )
    assert str(error.value) == expected_error_message
