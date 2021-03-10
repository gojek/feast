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
from typing import List, Optional

import pyarrow


class OfflineStore(ABC):
    """
    OfflineStore is a non-user-facing object used for all interaction between Feast and the service used for offline storage of features. Currently BigQuery is supported.
    """

    @staticmethod
    @abstractmethod
    def pull_table(
        table_ref: str,
        entity_names: List[str],
        feature_names: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> Optional[pyarrow.Table]:
        pass


class BigQueryOfflineStore(OfflineStore):
    """
    BigQueryOfflineStore is a non-user-facing object used for all interaction between Feast and BigQuery.
    """

    @staticmethod
    def pull_table(
        table_ref: str,
        entity_names: List[str],
        feature_names: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> pyarrow.Table:

        partition_by_entity_string = ", ".join(entity_names)
        if partition_by_entity_string != "":
            partition_by_entity_string = "PARTITION BY " + partition_by_entity_string
        feature_string = ", ".join(feature_names)
        timestamps = [event_timestamp_column]
        if created_timestamp_column is not None:
            timestamps.append(created_timestamp_column)
        timestamp_string = ", ".join(timestamps)
        timestamp_desc_string = " DESC, ".join(timestamps) + " DESC"
        field_string = ", ".join(entity_names + feature_names + timestamps)

        query = f"""
        SELECT {field_string}
        FROM (
            SELECT {field_string},
            ROW_NUMBER() OVER({partition_by_entity_string} ORDER BY {timestamp_desc_string}) AS _feast_row
            FROM `{table_ref}`
            WHERE {event_timestamp_column} BETWEEN TIMESTAMP('{start_date}') AND TIMESTAMP('{end_date}')
        )
        WHERE _feast_row = 1
        """
        return BigQueryOfflineStore._pull_query(query)

    @staticmethod
    def _pull_query(query: str) -> pyarrow.Table:
        from google.cloud import bigquery

        client = bigquery.Client()
        query_job = client.query(query)
        return query_job.to_arrow()
