import yaml
import json

import feast.specs.FeatureSpec_pb2 as feature_pb
from feast.types.Granularity_pb2 import Granularity
from feast.types.Value_pb2 import ValueType
from feast.sdk.resources.resource import FeastResource

from google.protobuf.json_format import MessageToJson, Parse

'''
Wrapper class for feast features
'''
class Feature(FeastResource):
    def __init__(self, name='', entity='', granularity=Granularity.NONE,
        owner='', warehouse_store=feature_pb.DataStore(),
        serving_store=feature_pb.DataStore(), description='', uri='',
        value_type=ValueType.DOUBLE, group='', tags=[], options={}):
        '''
        Args:
            name (str): name of feature, in lower snake case
            entity ({)str): entity the feature belongs to, in lower case
            granularity ({)int): granularity of the feature, one of Granularity.Enum
            owner (str): owner of the feature
            warehouse_store (FeatureSpec_pb2.DataStore): warehouse store id and options
            serving_store (FeatureSpec_pb2.DataStore): serving store id and options
            description (str): description of the feature (default: {""})
            uri (str): uri pointing to the source code or origin of this feature (default: {""})
            value_type ([type]): value type of the feature (default: {ValueType.DOUBLE})
            group (str): (optional) feature group to inherit from (default: {""})
            tags (list): (optional) tags assigned to the feature (default: {[]})
            options (dict): (optional) additional options for the feature (default: {{}})
        '''

        id = '.'.join([entity,
                       Granularity.Enum.Name(granularity), name]).lower()
        data_stores = feature_pb.DataStores(serving=serving_store, 
            warehouse=warehouse_store)
        self.__spec = feature_pb.FeatureSpec(id=id, granularity=granularity,
            name=name, entity=entity, owner=owner, dataStores=data_stores,
            description=description, uri=uri, valueType=value_type,
            group=group, tags=tags, options=options)

    @property
    def spec(self):
        return self.__spec

    @property
    def id(self):
        return self.__spec.id

    @property
    def name(self):
        return self.__spec.name

    @name.setter
    def name(self, value):
        self.__spec.name = value
        id_split = self.id.split('.')
        id_split[2] = value
        self.__spec.id = '.'.join(id_split)

    @property
    def granularity(self):
        return Granularity.Enum.Name(self.__spec.granularity)

    @granularity.setter
    def granularity(self, value):
        self.__spec.granularity = value
        id_split = self.id.split('.')
        id_split[1] = self.granularity.lower()
        self.__spec.id = '.'.join(id_split)

    @property
    def entity(self):
        return self.__spec.entity

    @entity.setter
    def entity(self, value):
        self.__spec.entity = value
        id_split = self.id.split('.')
        id_split[0] = value
        self.__spec.id = '.'.join(id_split)

    @property
    def owner(self):
        return self.__spec.owner

    @owner.setter
    def owner(self, value):
        self.__spec.owner = value

    @property
    def warehouse_store(self):
        return self.__spec.dataStores.warehouse

    @warehouse_store.setter
    def warehouse_store(self, value):
        self.__spec.dataStores.serving.CopyFrom(value)

    @property
    def serving_store(self):
        return self.__spec.dataStores.serving

    @serving_store.setter
    def serving_store(self, value):
        self.__spec.dataStores.warehouse.CopyFrom(value)

    @property
    def description(self):
        return self.__spec.description

    @description.setter
    def description(self, value):
        self.__spec.description = value

    @property
    def uri(self):
        return self.__spec.uri

    @uri.setter
    def uri(self, value):
        self.__spec.uri = value

    @property
    def value_type(self):
        return ValueType.Enum.Name(self.__spec.valueType)

    @value_type.setter
    def value_type(self, value):
        self.__spec.valueType = value

    @property
    def group(self):
        return self.__spec.group

    @group.setter
    def group(self, value):
        self.__spec.group = value

    @property
    def tags(self):
        return self.__spec.tags

    @tags.setter
    def tags(self, value):
        del self.__spec.tags[:]
        self.__spec.tags.extend(value)

    @property
    def options(self):
        return self.__spec.options

    @options.setter
    def options(self, value):
        for key in self.__spec.options:
            del self.__spec.options[key]
        for (key, value) in value.items():
            self.__spec.options[key] = value

    @classmethod
    def from_yaml_file(cls, path):
        '''Create an instance of feature from a yaml spec file
        
        Args:
            path (str): path to yaml spec file
        '''

        with open(path, 'r') as file:
            content = yaml.safe_load(file.read())
            feature = cls()
            feature.__spec = Parse(
                json.dumps(content),
                feature_pb.FeatureSpec(),
                ignore_unknown_fields=False)
            return feature
