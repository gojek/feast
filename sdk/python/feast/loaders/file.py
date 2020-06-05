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

import os
import shutil
import tempfile
import uuid
from datetime import datetime
from typing import List, Optional, Tuple, Union
from urllib.parse import urlparse

import pandas as pd
from pandavro import to_avro

from feast.staging.staging_strategy import StagingStrategy


def export_source_to_staging_location(
    source: Union[pd.DataFrame, str], staging_location_uri: str
) -> List[str]:
    """
    Uploads a DataFrame as an Avro file to a remote staging location.

    The local staging location specified in this function is used for E2E
    tests, please do not use it.

    Args:
        source (Union[pd.DataFrame, str]:
            Source of data to be staged. Can be a pandas DataFrame or a file
            path.

            Only three types of source are allowed:
                * Pandas DataFrame
                * Local Avro file
                * GCS Avro file
                * S3 Avro file


        staging_location_uri (str):
            Remote staging location where DataFrame should be written.
            Examples:
                * gs://bucket/path/
                * s3://bucket/path/
                * file:///data/subfolder/

    Returns:
        List[str]:
            Returns a list containing the full path to the file(s) in the
            remote staging location.
    """

    staging_strategy = StagingStrategy()
    uri = urlparse(staging_location_uri)

    # Prepare Avro file to be exported to staging location
    if isinstance(source, pd.DataFrame):
        # DataFrame provided as a source
        uri_path = None  # type: Optional[str]
        if uri.scheme == "file":
            uri_path = uri.path
        # Remote gs staging location provided by serving
        dir_path, file_name, source_path = export_dataframe_to_local(
            df=source, dir_path=uri_path
        )
    elif isinstance(source, str):
        if urlparse(source).scheme in ["", "file"]:
            # Local file provided as a source
            dir_path = None
            file_name = os.path.basename(source)
            source_path = os.path.abspath(
                os.path.join(urlparse(source).netloc, urlparse(source).path)
            )
        else:
            # gs, s3 file provided as a source.
            return staging_strategy.execute_get_source_files(source)
    else:
        raise Exception(
            f"Only string and DataFrame types are allowed as a "
            f"source, {type(source)} was provided."
        )

    # Push data to required staging location
    staging_strategy.execute_file_upload(
        uri.scheme,
        source_path,
        uri.hostname,
        str(uri.path).strip("/") + "/" + file_name,
    )

    # Clean up, remove local staging file
    if isinstance(source, pd.DataFrame) and len(str(dir_path)) > 4:
        shutil.rmtree(dir_path)

    return [staging_location_uri.rstrip("/") + "/" + file_name]


def export_dataframe_to_local(
    df: pd.DataFrame, dir_path: Optional[str] = None
) -> Tuple[str, str, str]:
    """
    Exports a pandas DataFrame to the local filesystem.

    Args:
        df (pd.DataFrame):
            Pandas DataFrame to save.

        dir_path (Optional[str]):
            Absolute directory path '/data/project/subfolder/'.

    Returns:
        Tuple[str, str, str]:
            Tuple of directory path, file name and destination path. The
            destination path can be obtained by concatenating the directory
            path and file name.
    """

    # Create local staging location if not provided
    if dir_path is None:
        dir_path = tempfile.mkdtemp()

    file_name = _get_file_name()
    dest_path = f"{dir_path}/{file_name}"

    # Temporarily rename datetime column to event_timestamp. Ideally we would
    # force the schema with our avro writer instead.
    df.columns = ["event_timestamp" if col == "datetime" else col for col in df.columns]

    try:
        # Export dataset to file in local path
        to_avro(df=df, file_path_or_buffer=dest_path)
    except Exception:
        raise
    finally:
        # Revert event_timestamp column to datetime
        df.columns = [
            "datetime" if col == "event_timestamp" else col for col in df.columns
        ]

    return dir_path, file_name, dest_path


def _get_file_name() -> str:
    """
    Create a random file name.

    Returns:
        str:
            Randomised file name.
    """

    return f'{datetime.now().strftime("%d-%m-%Y_%I-%M-%S_%p")}_{str(uuid.uuid4())[:8]}.avro'
