import uuid
from datetime import datetime
from typing import List, Optional, Union

import pandas as pd
import pyarrow as pa
from pydantic import StrictStr
from pydantic.typing import Literal

from feast.data_source import DataSource, RedshiftSource
from feast.feature_view import FeatureView
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.utils import aws_utils
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig


class RedshiftOfflineStoreConfig(FeastConfigBaseModel):
    """ Offline store config for AWS Redshift """

    type: Literal["redshift"] = "redshift"
    """ Offline store type selector"""

    cluster_id: StrictStr
    """ Redshift cluster identifier """

    region: StrictStr
    """ Redshift cluster's AWS region """

    user: StrictStr
    """ Redshift user name """

    database: StrictStr
    """ Redshift database name """

    s3_staging_location: StrictStr
    """ S3 path for importing & exporting data to Redshift """

    iam_role: StrictStr
    """ IAM Role for Redshift, granting it access to S3 """


class RedshiftOfflineStore(OfflineStore):
    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        assert isinstance(data_source, RedshiftSource)
        assert isinstance(config.offline_store, RedshiftOfflineStoreConfig)

        from_expression = data_source.get_table_query_string()

        partition_by_join_key_string = ", ".join(join_key_columns)
        if partition_by_join_key_string != "":
            partition_by_join_key_string = (
                "PARTITION BY " + partition_by_join_key_string
            )
        timestamp_columns = [event_timestamp_column]
        if created_timestamp_column:
            timestamp_columns.append(created_timestamp_column)
        timestamp_desc_string = " DESC, ".join(timestamp_columns) + " DESC"
        field_string = ", ".join(
            join_key_columns + feature_name_columns + timestamp_columns
        )

        redshift_client = aws_utils.get_redshift_data_client(
            config.offline_store.region
        )
        s3_resource = aws_utils.get_s3_resource(config.offline_store.region)

        query = f"""
            SELECT {field_string}
            FROM (
                SELECT {field_string},
                ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS _feast_row
                FROM {from_expression}
                WHERE {event_timestamp_column} BETWEEN TIMESTAMP '{start_date}' AND TIMESTAMP '{end_date}'
            )
            WHERE _feast_row = 1
            """
        return RedshiftRetrievalJob(
            query=query,
            redshift_client=redshift_client,
            s3_resource=s3_resource,
            config=config,
        )

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        pass

    @staticmethod
    def get_historical_features_by_view(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
        start_date: Optional[datetime],
        end_date: Optional[datetime],
        full_feature_names: bool,
    ):
        pass


class RedshiftRetrievalJob(RetrievalJob):
    def __init__(self, query: str, redshift_client, s3_resource, config: RepoConfig):
        """Initialize RedshiftRetrievalJob object.

        Args:
            query: Redshift SQL query to execute.
            redshift_client: boto3 redshift-data client
            s3_resource: boto3 s3 resource object
            config: Feast repo config
        """
        self.query = query
        self._redshift_client = redshift_client
        self._s3_resource = s3_resource
        self._config = config
        self._s3_path = (
            self._config.offline_store.s3_staging_location
            + "/unload/"
            + str(uuid.uuid4())
        )

    def to_df(self) -> pd.DataFrame:
        return aws_utils.unload_redshift_query_to_df(
            self._redshift_client,
            self._config.offline_store.cluster_id,
            self._config.offline_store.database,
            self._config.offline_store.user,
            self._s3_resource,
            self._s3_path,
            self._config.offline_store.iam_role,
            self.query,
        )

    def to_arrow(self) -> pa.Table:
        return aws_utils.unload_redshift_query_to_pa(
            self._redshift_client,
            self._config.offline_store.cluster_id,
            self._config.offline_store.database,
            self._config.offline_store.user,
            self._s3_resource,
            self._s3_path,
            self._config.offline_store.iam_role,
            self.query,
        )

    def to_s3(self) -> str:
        """ Export dataset to S3 in Parquet format and return path """
        aws_utils.execute_redshift_query_and_unload_to_s3(
            self._redshift_client,
            self._config.offline_store.cluster_id,
            self._config.offline_store.database,
            self._config.offline_store.user,
            self._s3_path,
            self._config.offline_store.iam_role,
            self.query,
        )
        return self._s3_path

    def to_redshift(self, table_name: str) -> None:
        """ Save dataset as a new Redshift table """
        aws_utils.execute_redshift_statement(
            self._redshift_client,
            self._config.offline_store.cluster_id,
            self._config.offline_store.database,
            self._config.offline_store.user,
            f'CREATE TABLE "{table_name}" AS ({self.query})',
        )
