# Data model

## Dataset

A dataset is a collection of rows that is produced by a historical retrieval from Feast in order to train a model. A dataset is produced by a join from one or more feature views onto an entity dataframe. Therefore, a dataset may consist of features from multiple feature views.

**Dataset vs Feature View:** Feature views contain the schema of data and a reference to where data can be found \(through its data source\). Datasets are the actual data manifestation of querying those data sources.

**Dataset vs Data Source:** Datasets are the output of historical retrieval, whereas data sources are the inputs. One or more data sources can be used in the creation of a dataset.

## Feature References

Feature references uniquely identify feature values in Feast. The structure of a feature reference in string form is as follows: `<feature_table>:<feature>`

Feature references are used for the retrieval of features from Feast:

```python
online_features = fs.get_online_features(
    features=[
        'driver_locations:lon',
        'drivers_activity:trips_today'
    ],
    entity_rows=[{'driver': 'driver_1001'}]
)
```

It is possible to retrieve features from multiple feature views with a single request, and Feast is able to join features from multiple tables in order to build a training dataset. However, It is not possible to reference \(or retrieve\) features from multiple projects at the same time.

## **Entity key**

Entity keys are one or more entity values that uniquely describe an entity. In the case of an entity \(like a `driver`\) that only has a single entity field, the entity _is_ an entity key. However, it is also possible for an entity key to consist of multiple entity values. For example, a feature view with the composite entity of \(customer, country\) might have an entity key of \(1001, 5\).

![](../.gitbook/assets/image%20%2815%29.png)

Entity keys act as primary keys. They are used during the lookup of features from the online store, and they are also used to match feature rows across feature views during point-in-time joins.

## Event timestamp

The timestamp on which an event occurred, as found in a feature view's data source. The entity timestamp describes the event time at which a feature was observed or generated.

Event timestamps are used during point-in-time joins to ensure that the latest feature values are joined from feature views onto entity rows. Event timestamps are also used to ensure that old feature values aren't served to models during online serving.

## Entity row

An entity key at a specific point in time.

![](../.gitbook/assets/image%20%2811%29.png)

## Entity dataframe

A collection of entity rows. Entity dataframes are the "left table" that is enriched with feature values when building training datasets. The entity dataframe is provided to Feast by users during historical retrieval:

```python
training_df = store.get_historical_features(
    entity_df=entity_df, 
    features = [
        'drivers_activity:trips_today'
        'drivers_activity:rating'
    ],
)
```

Example of an entity dataframe with feature values joined to it:

![](../.gitbook/assets/image%20%2817%29.png)

