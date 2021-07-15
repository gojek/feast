# Feature Service

### Feature Service

A feature service is an object that represents a logical group of features from one or more [feature views](feature-view.md). 
Feature Services allows features from within a feature view to be used as needed by an ML model. Users can expect to create one feature service per model, allowing for tracking of the features used by models.   

{% tabs %}
{% tab title="driver\_trips\_feature\_service.py" %}
```python
from driver_ratings_feature_view import driver_ratings_fv
from driver_trips_feature_view import driver_stats_fv

driver_stats_fs = FeatureService(
    name="driver_activity",
    features=[driver_stats_fv, driver_ratings_fv[["lifetime_rating"]]]
)
```
{% endtab %}
{% endtabs %}

Feature services are used during

* The generation of training datasets when querying feature views in order to find historical feature values. A single training dataset may consist of features from multiple feature views.
* Retrieval of features from the online store. The features retrieved from the online store may also belong to multiple feature views.

{% hint style="info" %}
Feast does not currently spin up any servers to serve these features.
{% endhint %}
