# Getting training features

Feast provides a historical retrieval interface for exporting feature data in order to train machine learning models. Essentially, users are able to enrich their data with features from any feature tables.

### Retrieving historical features

Below is an example of the process required to produce a training dataset:

```python
# Feature references with target feature
features = [
    "driver_trips:average_daily_rides",
    "driver_trips:maximum_daily_rides",
    "driver_trips:rating",
    "driver_trips:rating:trip_completed",
]

# Define entity source
entity_source = FileSource(
   "event_timestamp",
   ParquetFormat(),
   "gs://some-bucket/customer"
)

# Retrieve historical dataset from Feast.
historical_feature_retrieval_job = client.get_historical_features(
    features=features,
    entity_rows=entity_source
)

output_file_uri = historical_feature_retrieval_job.get_output_file_uri()
```

#### 1. Define feature references

[Feature references]() define the specific features that will be retrieved from Feast. These features can come from multiple feature tables. The only requirement is that the feature tables that make up the feature references have the same entity \(or composite entity\).

**2. Define an entity dataframe**

Feast needs to join feature values onto specific entities at specific points in time. Thus, it is necessary to provide an [entity dataframe]() as part of the `get_historical_features` method. In the example above we are defining an entity source. This source is an external file that provides Feast with the entity dataframe.

**3. Launch historical retrieval job**

Once the feature references and an entity source are defined, it is possible to call `get_historical_features()`.  This method launches a job that extracts features from the sources defined in the provided feature tables, joins them onto the provided entity source, and returns a reference to the training dataset that is produced.

Please see the [Feast SDK](https://api.docs.feast.dev/python) for more details.

### Point-in-time Joins

Feast always joins features onto entity data in a point-in-time correct way. The process can be described through an example.

In the example below there are two tables \(or dataframes\):

* The dataframe on the left is the [entity dataframe]() that contains timestamps, entities, and the target variable \(trip\_completed\). This dataframe is provided to Feast through an entity source.
* The dataframe on the right contains driver features. This dataframe is represented in Feast through a feature table and its accompanying data source\(s\).

The user would like to have the driver features joined onto the entity dataframe to produce a training dataset that contains both the target \(trip\_completed\) and features \(average\_daily\_rides, maximum\_daily\_rides, rating\). This dataset will then be used to train their model.

![](../../.gitbook/assets/point_in_time_join%20%281%29%20%282%29%20%282%29%20%283%29%20%283%29%20%283%29%20%283%29%20%282%29.png)

Feast is able to intelligently join feature data with different timestamps to a single entity dataframe. It does this through a point-in-time join as follows:

1. Feast loads the entity dataframe and all feature tables \(driver dataframe\) into the same location. This can either be a database or in memory.
2. For each [entity row]() in the [entity dataframe](getting-online-features.md), Feast tries to find feature values in each feature table to join to it. Feast extracts the timestamp and entity key of each row in the entity dataframe and scans backward through the feature table until it finds a matching entity key.
3. If the event timestamp of the matching entity key within the driver feature table is within the maximum age configured for the feature table, then the features at that entity key are joined onto the entity dataframe. If the event timestamp is outside of the maximum age, then only null values are returned.
4. If multiple entity keys are found with the same event timestamp, then they are deduplicated by the created timestamp, with newer values taking precedence.
5. Feast repeats this joining process for all feature tables and returns the resulting dataset.

{% hint style="info" %}
Point-in-time correct joins attempts to prevent the occurrence of feature leakage by trying to recreate the state of the world at a single point in time, instead of joining features based on exact timestamps only.
{% endhint %}

