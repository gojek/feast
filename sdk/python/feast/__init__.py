import logging

from pkg_resources import DistributionNotFound, get_distribution

from .client import Client
from .data_source import (
    BigQuerySource,
    FileSource,
    KafkaSource,
    KinesisSource,
    RedshiftSource,
    SourceType,
)
from .entity import Entity
from .feature import Feature
from .feature_service import FeatureService
from .feature_store import FeatureStore
from .feature_table import FeatureTable
from .feature_view import FeatureView
from .repo_config import RepoConfig
from .value_type import ValueType

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(message)s",
    datefmt="%m/%d/%Y %I:%M:%S %p",
    level=logging.INFO,
)

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    pass

__all__ = [
    "Client",
    "Entity",
    "BigQuerySource",
    "FileSource",
    "KafkaSource",
    "KinesisSource",
    "RedshiftSource",
    "Feature",
    "FeatureService",
    "FeatureStore",
    "FeatureTable",
    "FeatureView",
    "RepoConfig",
    "SourceType",
    "ValueType",
]
