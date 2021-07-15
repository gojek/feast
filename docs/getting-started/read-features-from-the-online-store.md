# Read features from the online store

The Feast Python SDK allows users to retrieve feature values from an online store. This API is used to look up feature values at low latency during model serving in order to make online predictions.

{% hint style="info" %}
Online stores only maintain the current state of features, i.e latest feature values. No historical data is stored or served.
{% endhint %}

### Retrieving online features

#### 1. Ensure that feature values have been loaded into the online store

Please ensure that you have materialized \(loaded\) your feature values into the online store before starting

{% page-ref page="load-data-into-the-online-store.md" %}

#### 2. Define feature references

Create a list of features that you would like to retrieve. This list typically comes from the model training step and should accompany the model binary.

```python
features = [
    "driver_hourly_stats:conv_rate",
    "driver_hourly_stats:acc_rate"
]
```

#### 3. Read online features

Next, we will create a feature store object and call `get_online_features()` which reads the relevant feature values directly from the online store.

```python
fs = FeatureStore(repo_path="path/to/feature/repo")
online_features = fs.get_online_features(
    features=features,
    entity_rows=[
        {"driver_id": 1001},
        {"driver_id": 1002}]
).to_dict()
```

```javascript
{
   "driver_hourly_stats__acc_rate":[
      0.2897740304470062,
      0.6447265148162842
   ],
   "driver_hourly_stats__conv_rate":[
      0.6508077383041382,
      0.14802511036396027
   ],
   "driver_id":[
      1001,
      1002
   ]
}
```

