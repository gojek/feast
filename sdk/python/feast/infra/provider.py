import abc
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import pandas
import pyarrow
from tqdm import tqdm

from feast import errors, importer
from feast.entity import Entity
from feast.feature_table import FeatureTable
from feast.feature_view import FeatureView
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.registry import Registry
from feast.repo_config import RepoConfig
from feast.type_map import python_value_to_proto_value

DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL = "event_timestamp"


class Provider(abc.ABC):
    @abc.abstractmethod
    def update_infra(
        self,
        project: str,
        tables_to_delete: Sequence[Union[FeatureTable, FeatureView]],
        tables_to_keep: Sequence[Union[FeatureTable, FeatureView]],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        """
        Reconcile cloud resources with the objects declared in the feature repo.

        Args:
            project: Project to which tables belong
            tables_to_delete: Tables that were deleted from the feature repo, so provider needs to
                clean up the corresponding cloud resources.
            tables_to_keep: Tables that are still in the feature repo. Depending on implementation,
                provider may or may not need to update the corresponding resources.
            entities_to_delete: Entities that were deleted from the feature repo, so provider needs to
                clean up the corresponding cloud resources.
            entities_to_keep: Entities that are still in the feature repo. Depending on implementation,
                provider may or may not need to update the corresponding resources.
            partial: if true, then tables_to_delete and tables_to_keep are *not* exhaustive lists.
                There may be other tables that are not touched by this update.
        """
        ...

    @abc.abstractmethod
    def teardown_infra(
        self,
        project: str,
        tables: Sequence[Union[FeatureTable, FeatureView]],
        entities: Sequence[Entity],
    ):
        """
        Tear down all cloud resources for a repo.

        Args:
            project: Feast project to which tables belong
            tables: Tables that are declared in the feature repo.
            entities: Entities that are declared in the feature repo.
        """
        ...

    @abc.abstractmethod
    def online_write_batch(
        self,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        """
        Write a batch of feature rows to the online store. This is a low level interface, not
        expected to be used by the users directly.

        If a tz-naive timestamp is passed to this method, it is assumed to be UTC.

        Args:
            config: The RepoConfig for the current FeatureStore.
            table: Feast FeatureTable
            data: a list of quadruplets containing Feature data. Each quadruplet contains an Entity Key,
                a dict containing feature values, an event timestamp for the row, and
                the created timestamp for the row if it exists.
            progress: Optional function to be called once every mini-batch of rows is written to
                the online store. Can be used to display progress.
        """
        ...

    @abc.abstractmethod
    def materialize_single_feature_view(
        self,
        config: RepoConfig,
        feature_view: FeatureView,
        start_date: datetime,
        end_date: datetime,
        registry: Registry,
        project: str,
        tqdm_builder: Callable[[int], tqdm],
    ) -> None:
        pass

    @abc.abstractmethod
    def get_historical_features(
        self,
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pandas.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool,
    ) -> RetrievalJob:
        pass

    @abc.abstractmethod
    def get_historical_features_by_view(
        self,
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pandas.DataFrame, str],
        registry: Registry,
        project: str,
        start_date: Optional[datetime],
        end_date: Optional[datetime],
        full_feature_names: bool,
    ) -> RetrievalJob:
        pass

    @abc.abstractmethod
    def online_read(
        self,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        entity_keys: List[EntityKeyProto],
        requested_features: List[str] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """
        Read feature values given an Entity Key. This is a low level interface, not
        expected to be used by the users directly.

        Returns:
            Data is returned as a list, one item per entity key. Each item in the list is a tuple
            of event_ts for the row, and the feature data as a dict from feature names to values.
            Values are returned as Value proto message.
        """
        ...


def get_provider(config: RepoConfig, repo_path: Path) -> Provider:
    if "." not in config.provider:
        if config.provider == "gcp":
            from feast.infra.gcp import GcpProvider

            return GcpProvider(config)
        elif config.provider == "aws":
            from feast.infra.aws import AwsProvider

            return AwsProvider(config)
        elif config.provider == "local":
            from feast.infra.local import LocalProvider

            return LocalProvider(config)
        else:
            raise errors.FeastProviderNotImplementedError(config.provider)
    else:
        # Split provider into module and class names by finding the right-most dot.
        # For example, provider 'foo.bar.MyProvider' will be parsed into 'foo.bar' and 'MyProvider'
        module_name, class_name = config.provider.rsplit(".", 1)

        cls = importer.get_class_from_type(module_name, class_name, "Provider")

        return cls(config, repo_path)


def _get_requested_feature_views_to_features_dict(
    feature_refs: List[str], feature_views: List[FeatureView]
) -> Dict[FeatureView, List[str]]:
    """Create a dict of FeatureView -> List[Feature] for all requested features.
    Set full_feature_names to True to have feature names prefixed by their feature view name."""

    feature_views_to_feature_map: Dict[FeatureView, List[str]] = {}

    for ref in feature_refs:
        ref_parts = ref.split(":")
        feature_view_from_ref = ref_parts[0]
        feature_from_ref = ref_parts[1]

        found = False
        for feature_view_from_registry in feature_views:
            if feature_view_from_registry.name == feature_view_from_ref:
                found = True
                if feature_view_from_registry in feature_views_to_feature_map:
                    feature_views_to_feature_map[feature_view_from_registry].append(
                        feature_from_ref
                    )
                else:
                    feature_views_to_feature_map[feature_view_from_registry] = [
                        feature_from_ref
                    ]

        if not found:
            raise ValueError(f"Could not find feature view from reference {ref}")

    return feature_views_to_feature_map


def _get_column_names(
    feature_view: FeatureView, entities: List[Entity]
) -> Tuple[List[str], List[str], str, Optional[str]]:
    """
    If a field mapping exists, run it in reverse on the join keys,
    feature names, event timestamp column, and created timestamp column
    to get the names of the relevant columns in the offline feature store table.

    Returns:
        Tuple containing the list of reverse-mapped join_keys,
        reverse-mapped feature names, reverse-mapped event timestamp column,
        and reverse-mapped created timestamp column that will be passed into
        the query to the offline store.
    """
    # if we have mapped fields, use the original field names in the call to the offline store
    event_timestamp_column = feature_view.input.event_timestamp_column
    feature_names = [feature.name for feature in feature_view.features]
    created_timestamp_column = feature_view.input.created_timestamp_column
    join_keys = [entity.join_key for entity in entities]
    if feature_view.input.field_mapping is not None:
        reverse_field_mapping = {
            v: k for k, v in feature_view.input.field_mapping.items()
        }
        event_timestamp_column = (
            reverse_field_mapping[event_timestamp_column]
            if event_timestamp_column in reverse_field_mapping.keys()
            else event_timestamp_column
        )
        created_timestamp_column = (
            reverse_field_mapping[created_timestamp_column]
            if created_timestamp_column
            and created_timestamp_column in reverse_field_mapping.keys()
            else created_timestamp_column
        )
        join_keys = [
            reverse_field_mapping[col] if col in reverse_field_mapping.keys() else col
            for col in join_keys
        ]
        feature_names = [
            reverse_field_mapping[col] if col in reverse_field_mapping.keys() else col
            for col in feature_names
        ]
    return (
        join_keys,
        feature_names,
        event_timestamp_column,
        created_timestamp_column,
    )


def _run_field_mapping(
    table: pyarrow.Table, field_mapping: Dict[str, str],
) -> pyarrow.Table:
    # run field mapping in the forward direction
    cols = table.column_names
    mapped_cols = [
        field_mapping[col] if col in field_mapping.keys() else col for col in cols
    ]
    table = table.rename_columns(mapped_cols)
    return table


def _convert_arrow_to_proto(
    table: pyarrow.Table, feature_view: FeatureView, join_keys: List[str],
) -> List[Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]]:
    rows_to_write = []

    def _coerce_datetime(ts):
        """
        Depending on underlying time resolution, arrow to_pydict() sometimes returns pandas
        timestamp type (for nanosecond resolution), and sometimes you get standard python datetime
        (for microsecond resolution).

        While pandas timestamp class is a subclass of python datetime, it doesn't always behave the
        same way. We convert it to normal datetime so that consumers downstream don't have to deal
        with these quirks.
        """

        if isinstance(ts, pandas.Timestamp):
            return ts.to_pydatetime()
        else:
            return ts

    for row in zip(*table.to_pydict().values()):
        entity_key = EntityKeyProto()
        for join_key in join_keys:
            entity_key.join_keys.append(join_key)
            idx = table.column_names.index(join_key)
            value = python_value_to_proto_value(row[idx])
            entity_key.entity_values.append(value)
        feature_dict = {}
        for feature in feature_view.features:
            idx = table.column_names.index(feature.name)
            value = python_value_to_proto_value(row[idx], feature.dtype)
            feature_dict[feature.name] = value
        event_timestamp_idx = table.column_names.index(
            feature_view.input.event_timestamp_column
        )
        event_timestamp = _coerce_datetime(row[event_timestamp_idx])

        if feature_view.input.created_timestamp_column:
            created_timestamp_idx = table.column_names.index(
                feature_view.input.created_timestamp_column
            )
            created_timestamp = _coerce_datetime(row[created_timestamp_idx])
        else:
            created_timestamp = None

        rows_to_write.append(
            (entity_key, feature_dict, event_timestamp, created_timestamp)
        )
    return rows_to_write
