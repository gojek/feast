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
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryFile
from typing import List, Optional
from urllib.parse import urlparse

from google.cloud.exceptions import NotFound

from feast.entity import Entity
from feast.errors import (
    EntityNotFoundException,
    FeatureTableNotFoundException,
    FeatureViewNotFoundException,
    S3RegistryBucketForbiddenAccess,
    S3RegistryBucketNotExist,
)
from feast.feature_table import FeatureTable
from feast.feature_view import FeatureView
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto

REGISTRY_SCHEMA_VERSION = "1"


class Registry:
    """
    Registry: A registry allows for the management and persistence of feature definitions and related metadata.
    """

    # The cached_registry_proto object is used for both reads and writes. In particular,
    # all write operations refresh the cache and modify it in memory; the write must
    # then be persisted to the underlying RegistryStore with a call to commit().
    cached_registry_proto: Optional[RegistryProto] = None
    cached_registry_proto_created: Optional[datetime] = None
    cached_registry_proto_ttl: timedelta
    cache_being_updated: bool = False

    def __init__(self, registry_path: str, repo_path: Path, cache_ttl: timedelta):
        """
        Create the Registry object.

        Args:
            repo_path: Path to the base of the Feast repository
            cache_ttl: The amount of time that cached registry state stays valid
            registry_path: filepath or GCS URI that is the location of the object store registry,
            or where it will be created if it does not exist yet.
        """
        uri = urlparse(registry_path)
        if uri.scheme == "gs":
            self._registry_store: RegistryStore = GCSRegistryStore(registry_path)
        elif uri.scheme == "s3":
            self._registry_store = S3RegistryStore(registry_path)
        elif uri.scheme == "file" or uri.scheme == "":
            self._registry_store = LocalRegistryStore(
                repo_path=repo_path, registry_path_string=registry_path
            )
        else:
            raise Exception(
                f"Registry path {registry_path} has unsupported scheme {uri.scheme}. Supported schemes are file and gs."
            )
        self.cached_registry_proto_ttl = cache_ttl

    def _initialize_registry(self):
        """Explicitly initializes the registry with an empty proto."""
        registry_proto = RegistryProto()
        registry_proto.registry_schema_version = REGISTRY_SCHEMA_VERSION
        self._registry_store.update_registry_proto(registry_proto)

    def apply_entity(self, entity: Entity, project: str, commit: bool = True):
        """
        Registers a single entity with Feast

        Args:
            entity: Entity that will be registered
            project: Feast project that this entity belongs to
            commit: Whether the change should be persisted immediately
        """
        entity.is_valid()
        entity_proto = entity.to_proto()
        entity_proto.spec.project = project
        self._prepare_registry_for_changes()
        assert self.cached_registry_proto

        for idx, existing_entity_proto in enumerate(
            self.cached_registry_proto.entities
        ):
            if (
                existing_entity_proto.spec.name == entity_proto.spec.name
                and existing_entity_proto.spec.project == project
            ):
                del self.cached_registry_proto.entities[idx]
                break

        self.cached_registry_proto.entities.append(entity_proto)
        if commit:
            self.commit()

    def list_entities(self, project: str, allow_cache: bool = False) -> List[Entity]:
        """
        Retrieve a list of entities from the registry

        Args:
            allow_cache: Whether to allow returning entities from a cached registry
            project: Filter entities based on project name

        Returns:
            List of entities
        """
        registry_proto = self._get_registry_proto(allow_cache=allow_cache)
        entities = []
        for entity_proto in registry_proto.entities:
            if entity_proto.spec.project == project:
                entities.append(Entity.from_proto(entity_proto))
        return entities

    def get_entity(self, name: str, project: str, allow_cache: bool = False) -> Entity:
        """
        Retrieves an entity.

        Args:
            name: Name of entity
            project: Feast project that this entity belongs to

        Returns:
            Returns either the specified entity, or raises an exception if
            none is found
        """
        registry_proto = self._get_registry_proto(allow_cache=allow_cache)
        for entity_proto in registry_proto.entities:
            if entity_proto.spec.name == name and entity_proto.spec.project == project:
                return Entity.from_proto(entity_proto)
        raise EntityNotFoundException(name, project=project)

    def apply_feature_table(
        self, feature_table: FeatureTable, project: str, commit: bool = True
    ):
        """
        Registers a single feature table with Feast

        Args:
            feature_table: Feature table that will be registered
            project: Feast project that this feature table belongs to
            commit: Whether the change should be persisted immediately
        """
        feature_table.is_valid()
        feature_table_proto = feature_table.to_proto()
        feature_table_proto.spec.project = project
        self._prepare_registry_for_changes()
        assert self.cached_registry_proto

        for idx, existing_feature_table_proto in enumerate(
            self.cached_registry_proto.feature_tables
        ):
            if (
                existing_feature_table_proto.spec.name == feature_table_proto.spec.name
                and existing_feature_table_proto.spec.project == project
            ):
                del self.cached_registry_proto.feature_tables[idx]
                break

        self.cached_registry_proto.feature_tables.append(feature_table_proto)
        if commit:
            self.commit()

    def apply_feature_view(
        self, feature_view: FeatureView, project: str, commit: bool = True
    ):
        """
        Registers a single feature view with Feast

        Args:
            feature_view: Feature view that will be registered
            project: Feast project that this feature view belongs to
            commit: Whether the change should be persisted immediately
        """
        feature_view.is_valid()
        feature_view_proto = feature_view.to_proto()
        feature_view_proto.spec.project = project
        self._prepare_registry_for_changes()
        assert self.cached_registry_proto

        for idx, existing_feature_view_proto in enumerate(
            self.cached_registry_proto.feature_views
        ):
            if (
                existing_feature_view_proto.spec.name == feature_view_proto.spec.name
                and existing_feature_view_proto.spec.project == project
            ):
                if FeatureView.from_proto(existing_feature_view_proto) == feature_view:
                    return
                else:
                    del self.cached_registry_proto.feature_views[idx]
                    break

        self.cached_registry_proto.feature_views.append(feature_view_proto)
        if commit:
            self.commit()

    def apply_materialization(
        self,
        feature_view: FeatureView,
        project: str,
        start_date: datetime,
        end_date: datetime,
        commit: bool = True,
    ):
        """
        Updates materialization intervals tracked for a single feature view in Feast

        Args:
            feature_view: Feature view that will be updated with an additional materialization interval tracked
            project: Feast project that this feature view belongs to
            start_date (datetime): Start date of the materialization interval to track
            end_date (datetime): End date of the materialization interval to track
            commit: Whether the change should be persisted immediately
        """
        self._prepare_registry_for_changes()
        assert self.cached_registry_proto

        for idx, existing_feature_view_proto in enumerate(
            self.cached_registry_proto.feature_views
        ):
            if (
                existing_feature_view_proto.spec.name == feature_view.name
                and existing_feature_view_proto.spec.project == project
            ):
                existing_feature_view = FeatureView.from_proto(
                    existing_feature_view_proto
                )
                existing_feature_view.materialization_intervals.append(
                    (start_date, end_date)
                )
                feature_view_proto = existing_feature_view.to_proto()
                feature_view_proto.spec.project = project
                del self.cached_registry_proto.feature_views[idx]
                self.cached_registry_proto.feature_views.append(feature_view_proto)
                if commit:
                    self.commit()
                return

        raise FeatureViewNotFoundException(feature_view.name, project)

    def list_feature_tables(self, project: str) -> List[FeatureTable]:
        """
        Retrieve a list of feature tables from the registry

        Args:
            project: Filter feature tables based on project name

        Returns:
            List of feature tables
        """
        registry_proto = self._get_registry_proto()
        feature_tables = []
        for feature_table_proto in registry_proto.feature_tables:
            if feature_table_proto.spec.project == project:
                feature_tables.append(FeatureTable.from_proto(feature_table_proto))
        return feature_tables

    def list_feature_views(
        self, project: str, allow_cache: bool = False
    ) -> List[FeatureView]:
        """
        Retrieve a list of feature views from the registry

        Args:
            allow_cache: Allow returning feature views from the cached registry
            project: Filter feature tables based on project name

        Returns:
            List of feature views
        """
        registry_proto = self._get_registry_proto(allow_cache=allow_cache)
        feature_views = []
        for feature_view_proto in registry_proto.feature_views:
            if feature_view_proto.spec.project == project:
                feature_views.append(FeatureView.from_proto(feature_view_proto))
        return feature_views

    def get_feature_table(self, name: str, project: str) -> FeatureTable:
        """
        Retrieves a feature table.

        Args:
            name: Name of feature table
            project: Feast project that this feature table belongs to

        Returns:
            Returns either the specified feature table, or raises an exception if
            none is found
        """
        registry_proto = self._get_registry_proto()
        for feature_table_proto in registry_proto.feature_tables:
            if (
                feature_table_proto.spec.name == name
                and feature_table_proto.spec.project == project
            ):
                return FeatureTable.from_proto(feature_table_proto)
        raise FeatureTableNotFoundException(name, project)

    def get_feature_view(self, name: str, project: str) -> FeatureView:
        """
        Retrieves a feature view.

        Args:
            name: Name of feature view
            project: Feast project that this feature view belongs to

        Returns:
            Returns either the specified feature view, or raises an exception if
            none is found
        """
        registry_proto = self._get_registry_proto()
        for feature_view_proto in registry_proto.feature_views:
            if (
                feature_view_proto.spec.name == name
                and feature_view_proto.spec.project == project
            ):
                return FeatureView.from_proto(feature_view_proto)
        raise FeatureViewNotFoundException(name, project)

    def delete_feature_table(self, name: str, project: str, commit: bool = True):
        """
        Deletes a feature table or raises an exception if not found.

        Args:
            name: Name of feature table
            project: Feast project that this feature table belongs to
            commit: Whether the change should be persisted immediately
        """
        self._prepare_registry_for_changes()
        assert self.cached_registry_proto

        for idx, existing_feature_table_proto in enumerate(
            self.cached_registry_proto.feature_tables
        ):
            if (
                existing_feature_table_proto.spec.name == name
                and existing_feature_table_proto.spec.project == project
            ):
                del self.cached_registry_proto.feature_tables[idx]
                if commit:
                    self.commit()
                return

        raise FeatureTableNotFoundException(name, project)

    def delete_feature_view(self, name: str, project: str, commit: bool = True):
        """
        Deletes a feature view or raises an exception if not found.

        Args:
            name: Name of feature view
            project: Feast project that this feature view belongs to
            commit: Whether the change should be persisted immediately
        """
        self._prepare_registry_for_changes()
        assert self.cached_registry_proto

        for idx, existing_feature_view_proto in enumerate(
            self.cached_registry_proto.feature_views
        ):
            if (
                existing_feature_view_proto.spec.name == name
                and existing_feature_view_proto.spec.project == project
            ):
                del self.cached_registry_proto.feature_views[idx]
                if commit:
                    self.commit()
                return

        raise FeatureViewNotFoundException(name, project)

    def commit(self):
        """Commits the state of the registry cache to the remote registry store."""
        if self.cached_registry_proto:
            self._registry_store.update_registry_proto(self.cached_registry_proto)

    def refresh(self):
        """Refreshes the state of the registry cache by fetching the registry state from the remote registry store."""
        self._get_registry_proto(allow_cache=False)

    def teardown(self):
        """Tears down all local and cloud resources."""
        self._registry_store.teardown()

    def _prepare_registry_for_changes(self):
        """Prepares the Registry for changes by refreshing the cache if necessary."""
        try:
            self._get_registry_proto(allow_cache=True)
        except FileNotFoundError:
            registry_proto = RegistryProto()
            registry_proto.registry_schema_version = REGISTRY_SCHEMA_VERSION
            self.cached_registry_proto = registry_proto
            self.cached_registry_proto_created = datetime.now()
        return self.cached_registry_proto

    def _get_registry_proto(self, allow_cache: bool = False) -> RegistryProto:
        """Returns the cached or remote registry state

        Args:
            allow_cache: Whether to allow the use of the registry cache when fetching the RegistryProto

        Returns: Returns a RegistryProto object which represents the state of the registry
        """
        expired = (
            self.cached_registry_proto is None
            or self.cached_registry_proto_created is None
        ) or (
            self.cached_registry_proto_ttl.total_seconds() > 0  # 0 ttl means infinity
            and (
                datetime.now()
                > (self.cached_registry_proto_created + self.cached_registry_proto_ttl)
            )
        )
        if allow_cache and (not expired or self.cache_being_updated):
            assert isinstance(self.cached_registry_proto, RegistryProto)
            return self.cached_registry_proto

        try:
            self.cache_being_updated = True
            registry_proto = self._registry_store.get_registry_proto()
            self.cached_registry_proto = registry_proto
            self.cached_registry_proto_created = datetime.now()
        except Exception as e:
            raise e
        finally:
            self.cache_being_updated = False
        return registry_proto


class RegistryStore(ABC):
    """
    RegistryStore: abstract base class implemented by specific backends (local file system, GCS)
    containing lower level methods used by the Registry class that are backend-specific.
    """

    @abstractmethod
    def get_registry_proto(self):
        """
        Retrieves the registry proto from the registry path. If there is no file at that path,
        raises a FileNotFoundError.

        Returns:
            Returns either the registry proto stored at the registry path, or an empty registry proto.
        """
        pass

    @abstractmethod
    def update_registry_proto(self, registry_proto: RegistryProto):
        """
        Overwrites the current registry proto with the proto passed in. This method
        writes to the registry path.

        Args:
            registry_proto: the new RegistryProto
        """
        pass

    @abstractmethod
    def teardown(self):
        """
        Tear down all resources.
        """
        pass


class LocalRegistryStore(RegistryStore):
    def __init__(self, repo_path: Path, registry_path_string: str):
        registry_path = Path(registry_path_string)
        if registry_path.is_absolute():
            self._filepath = registry_path
        else:
            self._filepath = repo_path.joinpath(registry_path)

    def get_registry_proto(self):
        registry_proto = RegistryProto()
        if self._filepath.exists():
            registry_proto.ParseFromString(self._filepath.read_bytes())
            return registry_proto
        raise FileNotFoundError(
            f'Registry not found at path "{self._filepath}". Have you run "feast apply"?'
        )

    def update_registry_proto(self, registry_proto: RegistryProto):
        self._write_registry(registry_proto)

    def teardown(self):
        try:
            self._filepath.unlink()
        except FileNotFoundError:
            # If the file deletion fails with FileNotFoundError, the file has already
            # been deleted.
            pass

    def _write_registry(self, registry_proto: RegistryProto):
        registry_proto.version_id = str(uuid.uuid4())
        registry_proto.last_updated.FromDatetime(datetime.utcnow())
        file_dir = self._filepath.parent
        file_dir.mkdir(exist_ok=True)
        self._filepath.write_bytes(registry_proto.SerializeToString())


class GCSRegistryStore(RegistryStore):
    def __init__(self, uri: str):
        try:
            from google.cloud import storage
        except ImportError as e:
            from feast.errors import FeastExtrasDependencyImportError

            raise FeastExtrasDependencyImportError("gcp", str(e))

        self.gcs_client = storage.Client()
        self._uri = urlparse(uri)
        self._bucket = self._uri.hostname
        self._blob = self._uri.path.lstrip("/")

    def get_registry_proto(self):
        from google.cloud import storage
        from google.cloud.exceptions import NotFound

        file_obj = TemporaryFile()
        registry_proto = RegistryProto()
        try:
            bucket = self.gcs_client.get_bucket(self._bucket)
        except NotFound:
            raise Exception(
                f"No bucket named {self._bucket} exists; please create it first."
            )
        if storage.Blob(bucket=bucket, name=self._blob).exists(self.gcs_client):
            self.gcs_client.download_blob_to_file(
                self._uri.geturl(), file_obj, timeout=30
            )
            file_obj.seek(0)
            registry_proto.ParseFromString(file_obj.read())
            return registry_proto
        raise FileNotFoundError(
            f'Registry not found at path "{self._uri.geturl()}". Have you run "feast apply"?'
        )

    def update_registry_proto(self, registry_proto: RegistryProto):
        self._write_registry(registry_proto)

    def teardown(self):
        gs_bucket = self.gcs_client.get_bucket(self._bucket)
        try:
            gs_bucket.delete_blob(self._blob)
        except NotFound:
            # If the blob deletion fails with NotFound, it has already been deleted.
            pass

    def _write_registry(self, registry_proto: RegistryProto):
        registry_proto.version_id = str(uuid.uuid4())
        registry_proto.last_updated.FromDatetime(datetime.utcnow())
        # we have already checked the bucket exists so no need to do it again
        gs_bucket = self.gcs_client.get_bucket(self._bucket)
        blob = gs_bucket.blob(self._blob)
        file_obj = TemporaryFile()
        file_obj.write(registry_proto.SerializeToString())
        file_obj.seek(0)
        blob.upload_from_file(file_obj)


class S3RegistryStore(RegistryStore):
    def __init__(self, uri: str):
        try:
            import boto3
        except ImportError as e:
            from feast.errors import FeastExtrasDependencyImportError

            raise FeastExtrasDependencyImportError("aws", str(e))
        self._uri = urlparse(uri)
        self._bucket = self._uri.hostname
        self._key = self._uri.path.lstrip("/")

        self.s3_client = boto3.resource(
            "s3", endpoint_url=os.environ.get("FEAST_S3_ENDPOINT_URL")
        )

    def get_registry_proto(self):
        file_obj = TemporaryFile()
        registry_proto = RegistryProto()
        try:
            from botocore.exceptions import ClientError
        except ImportError as e:
            from feast.errors import FeastExtrasDependencyImportError

            raise FeastExtrasDependencyImportError("aws", str(e))
        try:
            bucket = self.s3_client.Bucket(self._bucket)
            self.s3_client.meta.client.head_bucket(Bucket=bucket.name)
        except ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                raise S3RegistryBucketNotExist(self._bucket)
            else:
                raise S3RegistryBucketForbiddenAccess(self._bucket) from e

        try:
            obj = bucket.Object(self._key)
            obj.download_fileobj(file_obj)
            file_obj.seek(0)
            registry_proto.ParseFromString(file_obj.read())
            return registry_proto
        except ClientError as e:
            raise FileNotFoundError(
                f"Error while trying to locate Registry at path {self._uri.geturl()}"
            ) from e

    def update_registry_proto(self, registry_proto: RegistryProto):
        self._write_registry(registry_proto)

    def teardown(self):
        self.s3_client.Object(self._bucket, self._key).delete()

    def _write_registry(self, registry_proto: RegistryProto):
        registry_proto.version_id = str(uuid.uuid4())
        registry_proto.last_updated.FromDatetime(datetime.utcnow())
        # we have already checked the bucket exists so no need to do it again
        file_obj = TemporaryFile()
        file_obj.write(registry_proto.SerializeToString())
        file_obj.seek(0)
        self.s3_client.Bucket(self._bucket).put_object(Body=file_obj, Key=self._key)
