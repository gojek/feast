from datetime import datetime

import pandas as pd
from example import driver, driver_hourly_stats_view

from feast import FeatureStore


def main():
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)

    # Load the feature store from the current path
    fs = FeatureStore(repo_path=".")

    # Deploy the feature store to AWS
    print("Deploying feature store to AWS...")
    fs.apply([driver, driver_hourly_stats_view])

    # Select features
    feature_refs = ["driver_hourly_stats:conv_rate", "driver_hourly_stats:acc_rate"]

    print("Loading features into the online store...")
    fs.materialize_incremental(end_date=datetime.now())

    print("Retrieving online features...")

    # Retrieve features from the online store (DynamoDB)
    online_features = fs.get_online_features(
        features=feature_refs, entity_rows=[{"driver_id": 1001}, {"driver_id": 1002}],
    ).to_dict()

    print(pd.DataFrame.from_dict(online_features))


if __name__ == "__main__":
    main()
