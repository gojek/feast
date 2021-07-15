# Quickstart

In this tutorial we will

1. Deploy a local feature store with a **Parquet file offline store** and **Sqlite online store**.
2. Build a training dataset using our time series features from our **Parquet files**.
3. Materialize feature values from the offline store into the online store.
4. Read the latest features from the online store for inference.

## Install Feast

Install the Feast SDK and CLI using pip:

```bash
pip install feast
```

## Create a feature repository

Bootstrap a new feature repository using `feast init` from the command line:

```text
feast init feature_repo
cd feature_repo
```

```text
Creating a new Feast repository in /home/Jovyan/feature_repo.
```

## Register feature definitions and deploy your feature store

The `apply` command registers all the objects in your feature repository and deploys a feature store:

```bash
feast apply
```

```text
Registered entity driver_id
Registered feature view driver_hourly_stats
Deploying infrastructure for driver_hourly_stats
```

## Generating training data

The `apply` command builds a training dataset based on the time-series features defined in the feature repository:

```python
from datetime import datetime

import pandas as pd

from feast import FeatureStore

entity_df = pd.DataFrame.from_dict(
    {
        "driver_id": [1001, 1002, 1003, 1004],
        "event_timestamp": [
            datetime(2021, 4, 12, 10, 59, 42),
            datetime(2021, 4, 12, 8, 12, 10),
            datetime(2021, 4, 12, 16, 40, 26),
            datetime(2021, 4, 12, 15, 1, 12),
        ],
    }
)

store = FeatureStore(repo_path=".")

training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
    ],
).to_df()

print(training_df.head())
```

```bash
event_timestamp   driver_id  driver_hourly_stats__conv_rate  driver_hourly_stats__acc_rate  driver_hourly_stats__avg_daily_trips
2021-04-12        1002       0.328245                        0.993218                       329
2021-04-12        1001       0.448272                        0.873785                       767
2021-04-12        1004       0.822571                        0.571790                       673
2021-04-12        1003       0.556326                        0.605357                       335
```

## Load features into your online store

The `materialize` command loads the latest feature values from your feature views into your online store:

```bash
CURRENT_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S")
feast materialize-incremental $CURRENT_TIME
```

## Fetching feature vectors for inference

```python
from pprint import pprint
from feast import FeatureStore

store = FeatureStore(repo_path=".")

feature_vector = store.get_online_features(
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
    ],
    entity_rows=[{"driver_id": 1001}],
).to_dict()

pprint(feature_vector)
```

```python
{
    'driver_id': [1001],
    'conv_rate': [0.49274],
    'acc_rate': [0.92743],
    'avg_daily_trips': [72],
}
```

## Next steps

* Follow our [Getting Started](getting-started/) guide for a hands tutorial in using Feast
* Join other Feast users and contributors in [Slack](https://slack.feast.dev/) and become part of the community!

