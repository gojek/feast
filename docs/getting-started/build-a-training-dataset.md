# Build a training dataset

Feast allows users to build a training dataset from time-series feature data that already exists in an offline store. Users are expected to provide a list of features to retrieve \(which may span multiple feature views\), and a dataframe to join the resulting features onto. Feast will then execute a point-in-time join of multiple feature views onto the provided dataframe, and return the full resulting dataframe.

### Retrieving historical features

#### 1. Register your feature views

Please ensure that you have created a feature repository and that you have registered \(applied\) your feature views with Feast.

{% page-ref page="deploy-a-feature-store.md" %}

#### 2. Define feature references

Start by defining the feature references \(e.g., `driver_trips:average_daily_rides`\) for the features that you would like to retrieve from the offline store. These features can come from multiple feature tables. The only requirement is that the feature tables that make up the feature references have the same entity \(or composite entity\), and that they aren't located in the same offline store.

```python
feature_refs = [
    "driver_trips:average_daily_rides",
    "driver_trips:maximum_daily_rides",
    "driver_trips:rating",
    "driver_trips:rating:trip_completed",
]
```

**3. Create an entity dataframe**

An entity dataframe is the target dataframe on which you would like to join feature values. The entity dataframe must contain a timestamp column called `event_timestamp` and all entities \(primary keys\) necessary to join feature tables onto. All entities found in feature views that are being joined onto the entity dataframe must be found as column on the entity dataframe.

It is possible to provide entity dataframes as either a Pandas dataframe or a SQL query.

**Pandas:**

In the example below we create a Pandas based entity dataframe that has a single row with an `event_timestamp` column and a `driver_id` entity column. Pandas based entity dataframes may need to be uploaded into an offline store, which may result in longer wait times compared to a SQL based entity dataframe.

```python
import pandas as pd
from datetime import datetime

entity_df = pd.DataFrame(
    {
        "event_timestamp": [pd.Timestamp(datetime.now(), tz="UTC")],
        "driver_id": [1001]
    }
)
```

**SQL \(Alternative\):**

Below is an example of an entity dataframe built from a BigQuery SQL query. It is only possible to use this query when all feature views being queried are available in the same offline store \(BigQuery\).

```python
entity_df = "SELECT event_timestamp, driver_id FROM my_gcp_project.table"
```

**4. Launch historical retrieval**

```python
from feast import FeatureStore

fs = FeatureStore(repo_path="path/to/your/feature/repo")

training_df = fs.get_historical_features(
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate"
    ],
    entity_df=entity_df
).to_df()
```

Once the feature references and an entity dataframe are defined, it is possible to call `get_historical_features()`. This method launches a job that executes a point-in-time join of features from the offline store onto the entity dataframe. Once completed, a job reference will be returned. This job reference can then be converted to a Pandas dataframe by calling `to_df()`. 

