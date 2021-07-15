from datetime import datetime, timedelta

import pandas as pd
from driver_repo import driver, driver_stats_fv

from feast import FeatureStore


def main():
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)

    # Load the feature store from the current path
    fs = FeatureStore(repo_path=".")

    # Deploy the feature store to GCP
    print("Deploying feature store to GCP...")
    fs.apply([driver, driver_stats_fv])

    # Select features
    features = ["driver_hourly_stats:conv_rate", "driver_hourly_stats:acc_rate"]

    # Create an entity dataframe. This is the dataframe that will be enriched with historical features
    entity_df = pd.DataFrame(
        {
            "event_timestamp": [
                pd.Timestamp(dt, unit="ms", tz="UTC").round("ms")
                for dt in pd.date_range(
                    start=datetime.now() - timedelta(days=3),
                    end=datetime.now(),
                    periods=3,
                )
            ],
            "driver_id": [1001, 1002, 1003],
        }
    )

    print("Retrieving training data...")

    # Retrieve historical features by joining the entity dataframe to the BigQuery table source
    training_df = fs.get_historical_features(
        features=features, entity_df=entity_df
    ).to_df()

    print()
    print(training_df)

    print()
    print("Loading features into the online store...")
    fs.materialize_incremental(end_date=datetime.now())

    print()
    print("Retrieving online features...")

    # Retrieve features from the online store (Firestore)
    online_features = fs.get_online_features(
        features=features, entity_rows=[{"driver_id": 1001}, {"driver_id": 1002}],
    ).to_dict()

    print()
    print(pd.DataFrame.from_dict(online_features))


if __name__ == "__main__":
    main()
