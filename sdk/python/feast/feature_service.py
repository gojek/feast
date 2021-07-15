from datetime import datetime
from typing import Dict, List, Optional, Union

from google.protobuf.json_format import MessageToJson

from feast.feature_table import FeatureTable
from feast.feature_view import FeatureView
from feast.feature_view_projection import FeatureViewProjection
from feast.protos.feast.core.FeatureService_pb2 import (
    FeatureService as FeatureServiceProto,
)
from feast.protos.feast.core.FeatureService_pb2 import (
    FeatureServiceMeta,
    FeatureServiceSpec,
)


class FeatureService:
    """
    A feature service is a logical grouping of features for retrieval (training or serving).
    The features grouped by a feature service may come from any number of feature views.
    """

    name: str
    features: List[FeatureViewProjection]
    tags: Dict[str, str]
    created_timestamp: Optional[datetime] = None
    last_updated_timestamp: Optional[datetime] = None

    def __init__(
        self,
        name: str,
        features: List[Union[FeatureTable, FeatureView, FeatureViewProjection]],
        tags: Optional[Dict[str, str]] = None,
    ):
        """
        Create a new Feature Service object.

        Args:
            name: A unique name for the Feature Service.
            features: A list of Features that are grouped as part of this FeatureService.
                The list may contain Feature Views, Feature Tables, or a subset of either.
            tags: A dictionary of key-value pairs used for organizing Feature Services.
        """
        self.name = name
        self.features = []
        for feature in features:
            if isinstance(feature, FeatureTable) or isinstance(feature, FeatureView):
                self.features.append(FeatureViewProjection.from_definition(feature))
            elif isinstance(feature, FeatureViewProjection):
                self.features.append(feature)
            else:
                raise ValueError(f"Unexpected type: {type(feature)}")
        self.tags = tags or {}
        self.created_timestamp = None
        self.last_updated_timestamp = None

    def __repr__(self):
        items = (f"{k} = {v}" for k, v in self.__dict__.items())
        return f"<{self.__class__.__name__}({', '.join(items)})>"

    def __str__(self):
        return str(MessageToJson(self.to_proto()))

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if not isinstance(other, FeatureService):
            raise TypeError(
                "Comparisons should only involve FeatureService class objects."
            )
        if self.tags != other.tags or self.name != other.name:
            return False

        if sorted(self.features) != sorted(other.features):
            return False

        return True

    @staticmethod
    def from_proto(feature_service_proto: FeatureServiceProto):
        fs = FeatureService(
            name=feature_service_proto.spec.name,
            features=[
                FeatureViewProjection.from_proto(fp)
                for fp in feature_service_proto.spec.features
            ],
            tags=dict(feature_service_proto.spec.tags),
        )

        if feature_service_proto.meta.HasField("created_timestamp"):
            fs.created_timestamp = (
                feature_service_proto.meta.created_timestamp.ToDatetime()
            )
        if feature_service_proto.meta.HasField("last_updated_timestamp"):
            fs.last_updated_timestamp = (
                feature_service_proto.meta.last_updated_timestamp.ToDatetime()
            )

        return fs

    def to_proto(self) -> FeatureServiceProto:
        meta = FeatureServiceMeta()
        if self.created_timestamp:
            meta.created_timestamp.FromDatetime(self.created_timestamp)

        spec = FeatureServiceSpec()
        spec.name = self.name
        for definition in self.features:
            if isinstance(definition, FeatureTable) or isinstance(
                definition, FeatureView
            ):
                feature_ref = FeatureViewProjection(
                    definition.name, definition.features
                )
            else:
                feature_ref = definition

            spec.features.append(feature_ref.to_proto())

        if self.tags:
            spec.tags.update(self.tags)

        feature_service_proto = FeatureServiceProto(spec=spec, meta=meta)
        return feature_service_proto

    def validate(self):
        pass
