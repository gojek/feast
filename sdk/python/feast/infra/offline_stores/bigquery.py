import time
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from typing import List, Optional, Set, Union

import pandas
import pyarrow
from jinja2 import BaseLoader, Environment
from pandas import Timestamp
from pydantic import StrictStr
from pydantic.typing import Literal
from tenacity import Retrying, retry_if_exception_type, stop_after_delay, wait_fixed

from feast import errors
from feast.data_source import BigQuerySource, DataSource
from feast.errors import (
    BigQueryJobCancelled,
    BigQueryJobStillRunning,
    FeastProviderLoginError,
)
from feast.feature_view import FeatureView
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.provider import (
    DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
    _get_requested_feature_views_to_features_dict,
)
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig

try:
    from google.api_core.exceptions import NotFound
    from google.auth.exceptions import DefaultCredentialsError
    from google.cloud import bigquery
    from google.cloud.bigquery import Client, Table

except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("gcp", str(e))


class BigQueryOfflineStoreConfig(FeastConfigBaseModel):
    """ Offline store config for GCP BigQuery """

    type: Literal["bigquery"] = "bigquery"
    """ Offline store type selector"""

    dataset: StrictStr = "feast"
    """ (optional) BigQuery Dataset name for temporary tables """

    project_id: Optional[StrictStr] = None
    """ (optional) GCP project name used for the BigQuery offline store """


class BigQueryOfflineStore(OfflineStore):
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
        assert isinstance(data_source, BigQuerySource)
        from_expression = data_source.get_table_query_string()

        partition_by_join_key_string = ", ".join(join_key_columns)
        if partition_by_join_key_string != "":
            partition_by_join_key_string = (
                "PARTITION BY " + partition_by_join_key_string
            )
        timestamps = [event_timestamp_column]
        if created_timestamp_column:
            timestamps.append(created_timestamp_column)
        timestamp_desc_string = " DESC, ".join(timestamps) + " DESC"
        field_string = ", ".join(join_key_columns + feature_name_columns + timestamps)

        client = _get_bigquery_client(project=config.offline_store.project_id)
        query = f"""
            SELECT {field_string}
            FROM (
                SELECT {field_string},
                ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS _feast_row
                FROM {from_expression}
                WHERE {event_timestamp_column} BETWEEN TIMESTAMP('{start_date}') AND TIMESTAMP('{end_date}')
            )
            WHERE _feast_row = 1
            """
        return BigQueryRetrievalJob(query=query, client=client, config=config)

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pandas.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        # TODO: Add entity_df validation in order to fail before interacting with BigQuery
        assert isinstance(config.offline_store, BigQueryOfflineStoreConfig)

        client = _get_bigquery_client(project=config.offline_store.project_id)
        expected_join_keys = _get_join_keys(project, feature_views, registry)

        assert isinstance(config.offline_store, BigQueryOfflineStoreConfig)

        table = _upload_entity_df_into_bigquery(
            client=client,
            project=config.project,
            dataset_name=config.offline_store.dataset,
            dataset_project=client.project,
            entity_df=entity_df,
        )

        entity_df_event_timestamp_col = _infer_event_timestamp_from_bigquery_query(
            table.schema
        )
        _assert_expected_columns_in_bigquery(
            expected_join_keys, entity_df_event_timestamp_col, table.schema,
        )

        # Build a query context containing all information required to template the BigQuery SQL query
        query_context = get_feature_view_query_context(
            feature_refs,
            feature_views,
            registry,
            project,
            full_feature_names=full_feature_names,
        )

        # Infer min and max timestamps from entity_df to limit data read in BigQuery SQL query
        min_timestamp, max_timestamp = _get_entity_df_timestamp_bounds(
            client, str(table.reference), entity_df_event_timestamp_col
        )

        # Generate the BigQuery SQL query from the query context
        query = build_point_in_time_query(
            query_context,
            min_timestamp=min_timestamp,
            max_timestamp=max_timestamp,
            left_table_query_string=str(table.reference),
            entity_df_event_timestamp_col=entity_df_event_timestamp_col,
            full_feature_names=full_feature_names,
        )

        job = BigQueryRetrievalJob(query=query, client=client, config=config)
        return job

    @staticmethod
    def get_historical_features_by_view(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pandas.DataFrame, str],
        registry: Registry,
        project: str,
        start_date: Optional[datetime],
        end_date: Optional[datetime],
        full_feature_names: bool,
    ):
        raise NotImplementedError


def _assert_expected_columns_in_dataframe(
    join_keys: Set[str], entity_df_event_timestamp_col: str, entity_df: pandas.DataFrame
):
    entity_df_columns = set(entity_df.columns.values)
    expected_columns = join_keys.copy()
    expected_columns.add(entity_df_event_timestamp_col)

    missing_keys = expected_columns - entity_df_columns

    if len(missing_keys) != 0:
        raise errors.FeastEntityDFMissingColumnsError(expected_columns, missing_keys)


def _assert_expected_columns_in_bigquery(
    join_keys: Set[str], entity_df_event_timestamp_col: str, table_schema
):
    entity_columns = set()
    for schema_field in table_schema:
        entity_columns.add(schema_field.name)

    expected_columns = join_keys.copy()
    expected_columns.add(entity_df_event_timestamp_col)

    missing_keys = expected_columns - entity_columns

    if len(missing_keys) != 0:
        raise errors.FeastEntityDFMissingColumnsError(expected_columns, missing_keys)


def _get_join_keys(
    project: str, feature_views: List[FeatureView], registry: Registry
) -> Set[str]:
    join_keys = set()
    for feature_view in feature_views:
        entities = feature_view.entities
        for entity_name in entities:
            entity = registry.get_entity(entity_name, project)
            join_keys.add(entity.join_key)
    return join_keys


def _infer_event_timestamp_from_bigquery_query(table_schema) -> str:
    if any(
        schema_field.name == DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
        for schema_field in table_schema
    ):
        return DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
    else:
        datetime_columns = list(
            filter(
                lambda schema_field: schema_field.field_type == "TIMESTAMP",
                table_schema,
            )
        )
        if len(datetime_columns) == 1:
            print(
                f"Using {datetime_columns[0].name} as the event timestamp. To specify a column explicitly, please name it {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL}."
            )
            return datetime_columns[0].name
        else:
            raise ValueError(
                f"Please provide an entity_df with a column named {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL} representing the time of events."
            )


def _infer_event_timestamp_from_dataframe(entity_df: pandas.DataFrame) -> str:
    if DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL in entity_df.columns:
        return DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
    else:
        datetime_columns = entity_df.select_dtypes(
            include=["datetime", "datetimetz"]
        ).columns
        if len(datetime_columns) == 1:
            print(
                f"Using {datetime_columns[0]} as the event timestamp. To specify a column explicitly, please name it {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL}."
            )
            return datetime_columns[0]
        else:
            raise ValueError(
                f"Please provide an entity_df with a column named {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL} representing the time of events."
            )


class BigQueryRetrievalJob(RetrievalJob):
    def __init__(self, query, client, config):
        self.query = query
        self.client = client
        self.config = config

    def to_df(self):
        # TODO: Ideally only start this job when the user runs "get_historical_features", not when they run to_df()
        df = self.client.query(self.query).to_dataframe(create_bqstorage_client=True)
        return df

    def to_sql(self) -> str:
        """
        Returns the SQL query that will be executed in BigQuery to build the historical feature table.
        """
        return self.query

    def to_bigquery(
        self,
        job_config: bigquery.QueryJobConfig = None,
        timeout: int = 1800,
        retry_cadence: int = 10,
    ) -> Optional[str]:
        """
        Triggers the execution of a historical feature retrieval query and exports the results to a BigQuery table.
        Runs for a maximum amount of time specified by the timeout parameter (defaulting to 30 minutes).

        Args:
            job_config: An optional bigquery.QueryJobConfig to specify options like destination table, dry run, etc.
            timeout: An optional number of seconds for setting the time limit of the QueryJob.
            retry_cadence: An optional number of seconds for setting how long the job should checked for completion.

        Returns:
            Returns the destination table name or returns None if job_config.dry_run is True.
        """

        if not job_config:
            today = date.today().strftime("%Y%m%d")
            rand_id = str(uuid.uuid4())[:7]
            path = f"{self.client.project}.{self.config.offline_store.dataset}.historical_{today}_{rand_id}"
            job_config = bigquery.QueryJobConfig(destination=path)

        bq_job = self.client.query(self.query, job_config=job_config)

        if job_config.dry_run:
            print(
                "This query will process {} bytes.".format(bq_job.total_bytes_processed)
            )
            return None

        block_until_done(client=self.client, bq_job=bq_job, timeout=timeout)

        print(f"Done writing to '{job_config.destination}'.")
        return str(job_config.destination)

    def to_arrow(self) -> pyarrow.Table:
        return self.client.query(self.query).to_arrow()


def block_until_done(
    client: Client,
    bq_job: Union[bigquery.job.query.QueryJob, bigquery.job.load.LoadJob],
    timeout: int = 1800,
    retry_cadence: int = 10,
):
    """
    Waits for bq_job to finish running, up to a maximum amount of time specified by the timeout parameter (defaulting to 30 minutes).

    Args:
        client: A bigquery.client.Client to monitor the bq_job.
        bq_job: The bigquery.job.QueryJob that blocks until done runnning.
        timeout: An optional number of seconds for setting the time limit of the job.
        retry_cadence: An optional number of seconds for setting how long the job should checked for completion.

    Raises:
        BigQueryJobStillRunning exception if the function has blocked longer than 30 minutes.
        BigQueryJobCancelled exception to signify when that the job has been cancelled (i.e. from timeout or KeyboardInterrupt).
    """

    def _wait_until_done(job_id):
        if client.get_job(job_id).state in ["PENDING", "RUNNING"]:
            raise BigQueryJobStillRunning(job_id=job_id)

    job_id = bq_job.job_id
    try:
        retryer = Retrying(
            wait=wait_fixed(retry_cadence),
            stop=stop_after_delay(timeout),
            retry=retry_if_exception_type(BigQueryJobStillRunning),
            reraise=True,
        )
        retryer(_wait_until_done, job_id)

    finally:
        if client.get_job(job_id).state in ["PENDING", "RUNNING"]:
            client.cancel_job(job_id)
            raise BigQueryJobCancelled(job_id=job_id)

        if bq_job.exception():
            raise bq_job.exception()


@dataclass(frozen=True)
class FeatureViewQueryContext:
    """Context object used to template a BigQuery point-in-time SQL query"""

    name: str
    ttl: int
    entities: List[str]
    features: List[str]  # feature reference format
    table_ref: str
    event_timestamp_column: str
    created_timestamp_column: Optional[str]
    query: str
    table_subquery: str
    entity_selections: List[str]


def _get_table_id_for_new_entity(
    client: Client, project: str, dataset_name: str, dataset_project: str
) -> str:
    """Gets the table_id for the new entity to be uploaded."""

    # First create the BigQuery dataset if it doesn't exist
    dataset = bigquery.Dataset(f"{dataset_project}.{dataset_name}")
    dataset.location = "US"

    try:
        client.get_dataset(dataset)
    except NotFound:
        # Only create the dataset if it does not exist
        client.create_dataset(dataset, exists_ok=True)

    return f"{dataset_project}.{dataset_name}.entity_df_{project}_{int(time.time())}"


def _upload_entity_df_into_bigquery(
    client: Client,
    project: str,
    dataset_name: str,
    dataset_project: str,
    entity_df: Union[pandas.DataFrame, str],
) -> Table:
    """Uploads a Pandas entity dataframe into a BigQuery table and returns the resulting table"""

    table_id = _get_table_id_for_new_entity(
        client, project, dataset_name, dataset_project
    )

    if type(entity_df) is str:
        job = client.query(f"CREATE TABLE {table_id} AS ({entity_df})")
        block_until_done(client, job)
    elif isinstance(entity_df, pandas.DataFrame):
        # Drop the index so that we dont have unnecessary columns
        entity_df.reset_index(drop=True, inplace=True)

        # Upload the dataframe into BigQuery, creating a temporary table
        job_config = bigquery.LoadJobConfig()
        job = client.load_table_from_dataframe(
            entity_df, table_id, job_config=job_config
        )
        block_until_done(client, job)
    else:
        raise ValueError(
            f"The entity dataframe you have provided must be a Pandas DataFrame or BigQuery SQL query, "
            f"but we found: {type(entity_df)} "
        )

    # Ensure that the table expires after some time
    table = client.get_table(table=table_id)
    table.expires = datetime.utcnow() + timedelta(minutes=30)
    client.update_table(table, ["expires"])

    return table


def _get_entity_df_timestamp_bounds(
    client: Client, entity_df_bq_table: str, event_timestamp_col: str,
):

    boundary_df = (
        client.query(
            f"""
    SELECT
        MIN({event_timestamp_col}) AS min_timestamp,
        MAX({event_timestamp_col}) AS max_timestamp
    FROM {entity_df_bq_table}
    """
        )
        .result()
        .to_dataframe()
    )

    min_timestamp = boundary_df.loc[0, "min_timestamp"]
    max_timestamp = boundary_df.loc[0, "max_timestamp"]
    return min_timestamp, max_timestamp


def get_feature_view_query_context(
    feature_refs: List[str],
    feature_views: List[FeatureView],
    registry: Registry,
    project: str,
    full_feature_names: bool = False,
) -> List[FeatureViewQueryContext]:
    """Build a query context containing all information required to template a BigQuery point-in-time SQL query"""

    feature_views_to_feature_map = _get_requested_feature_views_to_features_dict(
        feature_refs, feature_views
    )

    query_context = []
    for feature_view, features in feature_views_to_feature_map.items():
        join_keys = []
        entity_selections = []
        reverse_field_mapping = {
            v: k for k, v in feature_view.input.field_mapping.items()
        }
        for entity_name in feature_view.entities:
            entity = registry.get_entity(entity_name, project)
            join_keys.append(entity.join_key)
            join_key_column = reverse_field_mapping.get(
                entity.join_key, entity.join_key
            )
            entity_selections.append(f"{join_key_column} AS {entity.join_key}")

        if isinstance(feature_view.ttl, timedelta):
            ttl_seconds = int(feature_view.ttl.total_seconds())
        else:
            ttl_seconds = 0

        assert isinstance(feature_view.input, BigQuerySource)

        event_timestamp_column = feature_view.input.event_timestamp_column
        created_timestamp_column = feature_view.input.created_timestamp_column

        context = FeatureViewQueryContext(
            name=feature_view.name,
            ttl=ttl_seconds,
            entities=join_keys,
            features=features,
            table_ref=feature_view.input.table_ref,
            event_timestamp_column=reverse_field_mapping.get(
                event_timestamp_column, event_timestamp_column
            ),
            created_timestamp_column=reverse_field_mapping.get(
                created_timestamp_column, created_timestamp_column
            ),
            # TODO: Make created column optional and not hardcoded
            query=feature_view.input.query,
            table_subquery=feature_view.input.get_table_query_string(),
            entity_selections=entity_selections,
        )
        query_context.append(context)
    return query_context


def build_point_in_time_query(
    feature_view_query_contexts: List[FeatureViewQueryContext],
    min_timestamp: Timestamp,
    max_timestamp: Timestamp,
    left_table_query_string: str,
    entity_df_event_timestamp_col: str,
    full_feature_names: bool = False,
):
    """Build point-in-time query between each feature view table and the entity dataframe"""
    template = Environment(loader=BaseLoader()).from_string(
        source=SINGLE_FEATURE_VIEW_POINT_IN_TIME_JOIN
    )

    # Add additional fields to dict
    template_context = {
        "min_timestamp": min_timestamp,
        "max_timestamp": max_timestamp,
        "left_table_query_string": left_table_query_string,
        "entity_df_event_timestamp_col": entity_df_event_timestamp_col,
        "unique_entity_keys": set(
            [entity for fv in feature_view_query_contexts for entity in fv.entities]
        ),
        "featureviews": [asdict(context) for context in feature_view_query_contexts],
        "full_feature_names": full_feature_names,
    }

    query = template.render(template_context)
    return query


def _get_bigquery_client(project: Optional[str] = None):
    try:
        client = bigquery.Client(project=project)
    except DefaultCredentialsError as e:
        raise FeastProviderLoginError(
            str(e)
            + '\nIt may be necessary to run "gcloud auth application-default login" if you would like to use your '
            "local Google Cloud account"
        )
    except EnvironmentError as e:
        raise FeastProviderLoginError(
            "GCP error: "
            + str(e)
            + "\nIt may be necessary to set a default GCP project by running "
            '"gcloud config set project your-project"'
        )

    return client


# TODO: Optimizations
#   * Use GENERATE_UUID() instead of ROW_NUMBER(), or join on entity columns directly
#   * Precompute ROW_NUMBER() so that it doesn't have to be recomputed for every query on entity_dataframe
#   * Create temporary tables instead of keeping all tables in memory

SINGLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
/*
 Compute a deterministic hash for the `left_table_query_string` that will be used throughout
 all the logic as the field to GROUP BY the data
*/
WITH entity_dataframe AS (
    SELECT
        *,
        CONCAT(
            {% for entity_key in unique_entity_keys %}
                CAST({{entity_key}} AS STRING),
            {% endfor %}
            CAST({{entity_df_event_timestamp_col}} AS STRING)
        ) AS entity_row_unique_id
    FROM {{ left_table_query_string }}
),

{% for featureview in featureviews %}

/*
 This query template performs the point-in-time correctness join for a single feature set table
 to the provided entity table.

 1. We first join the current feature_view to the entity dataframe that has been passed.
 This JOIN has the following logic:
    - For each row of the entity dataframe, only keep the rows where the `event_timestamp_column`
    is less than the one provided in the entity dataframe
    - If there a TTL for the current feature_view, also keep the rows where the `event_timestamp_column`
    is higher the the one provided minus the TTL
    - For each row, Join on the entity key and retrieve the `entity_row_unique_id` that has been
    computed previously

 The output of this CTE will contain all the necessary information and already filtered out most
 of the data that is not relevant.
*/

{{ featureview.name }}__subquery AS (
    SELECT
        {{ featureview.event_timestamp_column }} as event_timestamp,
        {{ featureview.created_timestamp_column ~ ' as created_timestamp,' if featureview.created_timestamp_column else '' }}
        {{ featureview.entity_selections | join(', ')}},
        {% for feature in featureview.features %}
            {{ feature }} as {% if full_feature_names %}{{ featureview.name }}__{{feature}}{% else %}{{ feature }}{% endif %}{% if loop.last %}{% else %}, {% endif %}
        {% endfor %}
    FROM {{ featureview.table_subquery }}
    WHERE {{ featureview.event_timestamp_column }} <= '{{max_timestamp}}'
    {% if featureview.ttl == 0 %}{% else %}
    AND {{ featureview.event_timestamp_column }} >= Timestamp_sub('{{min_timestamp}}', interval {{ featureview.ttl }} second)
    {% endif %}
),

{{ featureview.name }}__base AS (
    SELECT
        subquery.*,
        entity_dataframe.{{entity_df_event_timestamp_col}} AS entity_timestamp,
        entity_dataframe.entity_row_unique_id
    FROM {{ featureview.name }}__subquery AS subquery
    INNER JOIN entity_dataframe
    ON TRUE
        AND subquery.event_timestamp <= entity_dataframe.{{entity_df_event_timestamp_col}}

        {% if featureview.ttl == 0 %}{% else %}
        AND subquery.event_timestamp >= Timestamp_sub(entity_dataframe.{{entity_df_event_timestamp_col}}, interval {{ featureview.ttl }} second)
        {% endif %}

        {% for entity in featureview.entities %}
        AND subquery.{{ entity }} = entity_dataframe.{{ entity }}
        {% endfor %}
),

/*
 2. If the `created_timestamp_column` has been set, we need to
 deduplicate the data first. This is done by calculating the
 `MAX(created_at_timestamp)` for each event_timestamp.
 We then join the data on the next CTE
*/
{% if featureview.created_timestamp_column %}
{{ featureview.name }}__dedup AS (
    SELECT
        entity_row_unique_id,
        event_timestamp,
        MAX(created_timestamp) as created_timestamp,
    FROM {{ featureview.name }}__base
    GROUP BY entity_row_unique_id, event_timestamp
),
{% endif %}

/*
 3. The data has been filtered during the first CTE "*__base"
 Thus we only need to compute the latest timestamp of each feature.
*/
{{ featureview.name }}__latest AS (
    SELECT
        entity_row_unique_id,
        MAX(event_timestamp) AS event_timestamp
        {% if featureview.created_timestamp_column %}
            ,ANY_VALUE(created_timestamp) AS created_timestamp
        {% endif %}

    FROM {{ featureview.name }}__base
    {% if featureview.created_timestamp_column %}
        INNER JOIN {{ featureview.name }}__dedup
        USING (entity_row_unique_id, event_timestamp, created_timestamp)
    {% endif %}

    GROUP BY entity_row_unique_id
),

/*
 4. Once we know the latest value of each feature for a given timestamp,
 we can join again the data back to the original "base" dataset
*/
{{ featureview.name }}__cleaned AS (
    SELECT base.*
    FROM {{ featureview.name }}__base as base
    INNER JOIN {{ featureview.name }}__latest
    USING(
        entity_row_unique_id,
        event_timestamp
        {% if featureview.created_timestamp_column %}
            ,created_timestamp
        {% endif %}
    )
){% if loop.last %}{% else %}, {% endif %}


{% endfor %}
/*
 Joins the outputs of multiple time travel joins to a single table.
 The entity_dataframe dataset being our source of truth here.
 */

SELECT * EXCEPT (entity_row_unique_id)
FROM entity_dataframe
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
        entity_row_unique_id,
        {% for feature in featureview.features %}
            {% if full_feature_names %}{{ featureview.name }}__{{feature}}{% else %}{{ feature }}{% endif %},
        {% endfor %}
    FROM {{ featureview.name }}__cleaned
) USING (entity_row_unique_id)
{% endfor %}
"""
