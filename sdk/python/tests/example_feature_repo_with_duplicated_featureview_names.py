from google.protobuf.duration_pb2 import Duration

from feast import FeatureView, FileSource

driver_hourly_stats = FileSource(
    path="driver_stats.parquet",  # this parquet is not real and will not be read
)

driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",  # Intentionally use the same FeatureView name
    entities=["driver_id"],
    online=False,
    input=driver_hourly_stats,
    ttl=Duration(seconds=10),
    tags={},
)

driver_hourly_stats_view_dup1 = FeatureView(
    name="driver_hourly_stats",  # Intentionally use the same FeatureView name
    entities=["driver_id"],
    online=False,
    input=driver_hourly_stats,
    ttl=Duration(seconds=10),
    tags={},
)
