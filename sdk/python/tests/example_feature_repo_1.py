from datetime import timedelta

from feast import (
    BigQuerySource,
    Entity,
    Feature,
    FeatureService,
    FeatureView,
    ValueType,
)

driver_locations_source = BigQuerySource(
    table_ref="feast-oss.public.drivers",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created_timestamp",
)

customer_profile_source = BigQuerySource(
    table_ref="feast-oss.public.customers", event_timestamp_column="event_timestamp",
)

customer_driver_combined_source = BigQuerySource(
    table_ref="feast-oss.public.customer_driver",
    event_timestamp_column="event_timestamp",
)

driver = Entity(
    name="driver",  # The name is derived from this argument, not object name.
    value_type=ValueType.INT64,
    description="driver id",
)

customer = Entity(
    name="customer",  # The name is derived from this argument, not object name.
    value_type=ValueType.STRING,
)


driver_locations = FeatureView(
    name="driver_locations",
    entities=["driver"],
    ttl=timedelta(days=1),
    features=[
        Feature(name="lat", dtype=ValueType.FLOAT),
        Feature(name="lon", dtype=ValueType.STRING),
    ],
    online=True,
    input=driver_locations_source,
    tags={},
)

customer_profile = FeatureView(
    name="customer_profile",
    entities=["customer"],
    ttl=timedelta(days=1),
    features=[
        Feature(name="avg_orders_day", dtype=ValueType.FLOAT),
        Feature(name="name", dtype=ValueType.STRING),
        Feature(name="age", dtype=ValueType.INT64),
    ],
    online=True,
    input=customer_profile_source,
    tags={},
)

customer_driver_combined = FeatureView(
    name="customer_driver_combined",
    entities=["customer", "driver"],
    ttl=timedelta(days=1),
    features=[Feature(name="trips", dtype=ValueType.INT64)],
    online=True,
    input=customer_driver_combined_source,
    tags={},
)


all_drivers_feature_service = FeatureService(
    name="driver_locations_service",
    features=[driver_locations],
    tags={"release": "production"},
)

partial_drivers_feature_service = FeatureService(
    name="driver_latitudes_service", features=[driver_locations[["lat"]]]
)
