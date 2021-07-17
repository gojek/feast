# Copyright 2019 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional, Union

import pandas as pd
import pyarrow

from feast.data_source import DataSource
from feast.feature_view import FeatureView
from feast.registry import Registry
from feast.repo_config import RepoConfig


class RetrievalJob(ABC):
    """RetrievalJob is used to manage the execution of a historical feature retrieval"""

    @abstractmethod
    def to_df(self) -> pd.DataFrame:
        """Return dataset as Pandas DataFrame synchronously"""
        pass

    @abstractmethod
    def to_arrow(self) -> pyarrow.Table:
        """Return dataset as pyarrow Table synchronously"""
        pass


class OfflineStore(ABC):
    """
    OfflineStore is an object used for all interaction between Feast and the service used for offline storage of
    features.
    """

    @staticmethod
    @abstractmethod
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
        """
        Note that join_key_columns, feature_name_columns, event_timestamp_column, and created_timestamp_column
        have all already been mapped to column names of the source table and those column names are the values passed
        into this function.
        """
        pass

    @staticmethod
    @abstractmethod
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
