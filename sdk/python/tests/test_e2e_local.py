import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from pytz import utc

import feast.driver_test_data as driver_data
from feast import FeatureStore
from tests.cli_utils import CliRunner, get_example_repo


def _get_last_feature_row(df: pd.DataFrame, driver_id, max_date: datetime):
    """ Manually extract last feature value from a dataframe for a given driver_id with up to `max_date` date """
    filtered = df[
        (df["driver_id"] == driver_id) & (df["datetime"] < max_date.replace(tzinfo=utc))
    ]
    max_ts = filtered.loc[filtered["datetime"].idxmax()]["datetime"]
    filtered_by_ts = filtered[filtered["datetime"] == max_ts]
    return filtered_by_ts.loc[filtered_by_ts["created"].idxmax()]


def _assert_online_features(
    store: FeatureStore, driver_df: pd.DataFrame, max_date: datetime
):
    """Assert that features in online store are up to date with `max_date` date."""
    # Read features back
    result = store.get_online_features(
        features=[
            "driver_hourly_stats:conv_rate",
            "driver_hourly_stats:avg_daily_trips",
        ],
        entity_rows=[{"driver_id": 1001}],
        full_feature_names=True,
    )

    assert "driver_hourly_stats__avg_daily_trips" in result.to_dict()

    assert "driver_hourly_stats__conv_rate" in result.to_dict()
    assert (
        abs(
            result.to_dict()["driver_hourly_stats__conv_rate"][0]
            - _get_last_feature_row(driver_df, 1001, max_date)["conv_rate"]
        )
        < 0.01
    )


def test_e2e_local() -> None:
    """
    A more comprehensive than "basic" test, using local provider.

    1. Create a repo.
    2. Apply
    3. Ingest some data to online store from parquet
    4. Read from the online store to make sure it made it there.
    """

    runner = CliRunner()
    with tempfile.TemporaryDirectory() as data_dir:

        # Generate some test data in parquet format.
        end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
        start_date = end_date - timedelta(days=15)

        driver_entities = [1001, 1002, 1003, 1004, 1005]
        driver_df = driver_data.create_driver_hourly_stats_df(
            driver_entities, start_date, end_date
        )

        driver_stats_path = os.path.join(data_dir, "driver_stats.parquet")
        driver_df.to_parquet(path=driver_stats_path, allow_truncated_timestamps=True)

        # Note that runner takes care of running apply/teardown for us here.
        # We patch python code in example_feature_repo_2.py to set the path to Parquet files.
        with runner.local_repo(
            get_example_repo("example_feature_repo_2.py").replace(
                "%PARQUET_PATH%", driver_stats_path
            ),
            "file",
        ) as store:

            assert store.repo_path is not None

            # feast materialize
            r = runner.run(
                [
                    "materialize",
                    start_date.isoformat(),
                    (end_date - timedelta(days=7)).isoformat(),
                ],
                cwd=Path(store.repo_path),
            )

            assert r.returncode == 0

            _assert_online_features(store, driver_df, end_date - timedelta(days=7))

            # feast materialize-incremental
            r = runner.run(
                ["materialize-incremental", end_date.isoformat()],
                cwd=Path(store.repo_path),
            )

            assert r.returncode == 0

            _assert_online_features(store, driver_df, end_date)

        # Test a failure case when the parquet file doesn't include a join key
        with runner.local_repo(
            get_example_repo("example_feature_repo_with_entity_join_key.py").replace(
                "%PARQUET_PATH%", driver_stats_path
            ),
            "file",
        ) as store:

            assert store.repo_path is not None

            # feast materialize
            returncode, output = runner.run_with_output(
                [
                    "materialize",
                    start_date.isoformat(),
                    (end_date - timedelta(days=7)).isoformat(),
                ],
                cwd=Path(store.repo_path),
            )

            assert returncode != 0
            assert "feast.errors.FeastJoinKeysDuringMaterialization" in str(output)
