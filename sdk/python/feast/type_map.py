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

import re
from typing import Any, Dict, Union

import numpy as np
import pandas as pd
from google.protobuf.json_format import MessageToDict

from feast.protos.feast.types.Value_pb2 import (
    BoolList,
    BytesList,
    DoubleList,
    FloatList,
    Int32List,
    Int64List,
    StringList,
)
from feast.protos.feast.types.Value_pb2 import Value as ProtoValue
from feast.value_type import ValueType


def feast_value_type_to_python_type(field_value_proto: ProtoValue) -> Any:
    """
    Converts field value Proto to Dict and returns each field's Feast Value Type value
    in their respective Python value.

    Args:
        field_value_proto: Field value Proto

    Returns:
        Python native type representation/version of the given field_value_proto
    """
    field_value_dict = MessageToDict(field_value_proto)

    for k, v in field_value_dict.items():
        if k == "int64Val":
            return int(v)
        if k == "bytesVal":
            return bytes(v)
        if (k == "int64ListVal") or (k == "int32ListVal"):
            return [int(item) for item in v["val"]]
        if (k == "floatListVal") or (k == "doubleListVal"):
            return [float(item) for item in v["val"]]
        if k == "stringListVal":
            return [str(item) for item in v["val"]]
        if k == "bytesListVal":
            return [bytes(item) for item in v["val"]]
        if k == "boolListVal":
            return [bool(item) for item in v["val"]]

        if k in ["int32Val", "floatVal", "doubleVal", "stringVal", "boolVal"]:
            return v
        else:
            raise TypeError(
                f"Casting to Python native type for type {k} failed. "
                f"Type {k} not found"
            )


def python_type_to_feast_value_type(
    name: str, value, recurse: bool = True
) -> ValueType:
    """
    Finds the equivalent Feast Value Type for a Python value. Both native
    and Pandas types are supported. This function will recursively look
    for nested types when arrays are detected. All types must be homogenous.

    Args:
        name: Name of the value or field
        value: Value that will be inspected
        recurse: Whether to recursively look for nested types in arrays

    Returns:
        Feast Value Type
    """

    type_name = type(value).__name__

    type_map = {
        "int": ValueType.INT64,
        "str": ValueType.STRING,
        "float": ValueType.DOUBLE,
        "bytes": ValueType.BYTES,
        "float64": ValueType.DOUBLE,
        "float32": ValueType.FLOAT,
        "int64": ValueType.INT64,
        "uint64": ValueType.INT64,
        "int32": ValueType.INT32,
        "uint32": ValueType.INT32,
        "uint8": ValueType.INT32,
        "int8": ValueType.INT32,
        "bool": ValueType.BOOL,
        "timedelta": ValueType.UNIX_TIMESTAMP,
        "datetime64[ns]": ValueType.UNIX_TIMESTAMP,
        "datetime64[ns, tz]": ValueType.UNIX_TIMESTAMP,
        "category": ValueType.STRING,
    }

    if type_name in type_map:
        return type_map[type_name]

    if type_name == "ndarray" or isinstance(value, list):
        if recurse:

            # Convert to list type
            list_items = pd.core.series.Series(value)

            # This is the final type which we infer from the list
            common_item_value_type = None
            for item in list_items:
                if isinstance(item, ProtoValue):
                    current_item_value_type = _proto_str_to_value_type(
                        str(item.WhichOneof("val"))
                    )
                else:
                    # Get the type from the current item, only one level deep
                    current_item_value_type = python_type_to_feast_value_type(
                        name=name, value=item, recurse=False
                    )
                # Validate whether the type stays consistent
                if (
                    common_item_value_type
                    and not common_item_value_type == current_item_value_type
                ):
                    raise ValueError(
                        f"List value type for field {name} is inconsistent. "
                        f"{common_item_value_type} different from "
                        f"{current_item_value_type}."
                    )
                common_item_value_type = current_item_value_type
            if common_item_value_type is None:
                raise ValueError(
                    f"field {name} cannot have null values for type inference."
                )
            return ValueType[common_item_value_type.name + "_LIST"]
        else:
            raise ValueError(
                f"Value type for field {name} is {value.dtype.__str__()} but "
                f"recursion is not allowed. Array types can only be one level "
                f"deep."
            )

    return type_map[value.dtype.__str__()]


def _type_err(item, dtype):
    raise ValueError(f'Value "{item}" is of type {type(item)} not of type {dtype}')


def _python_value_to_proto_value(feast_value_type, value) -> ProtoValue:
    """
    Converts a Python (native, pandas) value to a Feast Proto Value based
    on a provided value type

    Args:
        feast_value_type: The target value type
        value: Value that will be converted

    Returns:
        Feast Value Proto
    """

    # Detect list type and handle separately
    if "list" in feast_value_type.name.lower():

        if feast_value_type == ValueType.FLOAT_LIST:
            return ProtoValue(
                float_list_val=FloatList(
                    val=[
                        item
                        if type(item) in [np.float32, np.float64, float]
                        else _type_err(item, np.float32)
                        for item in value
                    ]
                )
            )

        if feast_value_type == ValueType.DOUBLE_LIST:
            return ProtoValue(
                double_list_val=DoubleList(
                    val=[
                        item
                        if type(item) in [np.float64, np.float32, float]
                        else _type_err(item, np.float64)
                        for item in value
                    ]
                )
            )

        if feast_value_type == ValueType.INT32_LIST:
            return ProtoValue(
                int32_list_val=Int32List(
                    val=[
                        item if type(item) is np.int32 else _type_err(item, np.int32)
                        for item in value
                    ]
                )
            )

        if feast_value_type == ValueType.INT64_LIST:
            return ProtoValue(
                int64_list_val=Int64List(
                    val=[
                        item
                        if type(item) in [np.int64, np.int32]
                        else _type_err(item, np.int64)
                        for item in value
                    ]
                )
            )

        if feast_value_type == ValueType.UNIX_TIMESTAMP_LIST:
            return ProtoValue(
                int64_list_val=Int64List(
                    val=[
                        item
                        if type(item) in [np.int64, np.int32]
                        else _type_err(item, np.int64)
                        for item in value
                    ]
                )
            )

        if feast_value_type == ValueType.STRING_LIST:
            return ProtoValue(
                string_list_val=StringList(
                    val=[
                        item
                        if type(item) in [np.str_, str]
                        else _type_err(item, np.str_)
                        for item in value
                    ]
                )
            )

        if feast_value_type == ValueType.BOOL_LIST:
            return ProtoValue(
                bool_list_val=BoolList(
                    val=[
                        item
                        if type(item) in [np.bool_, bool]
                        else _type_err(item, np.bool_)
                        for item in value
                    ]
                )
            )

        if feast_value_type == ValueType.BYTES_LIST:
            return ProtoValue(
                bytes_list_val=BytesList(
                    val=[
                        item
                        if type(item) in [np.bytes_, bytes]
                        else _type_err(item, np.bytes_)
                        for item in value
                    ]
                )
            )

    # Handle scalar types below
    else:
        if pd.isnull(value):
            return ProtoValue()
        elif feast_value_type == ValueType.INT32:
            return ProtoValue(int32_val=int(value))
        elif feast_value_type == ValueType.INT64:
            return ProtoValue(int64_val=int(value))
        elif feast_value_type == ValueType.UNIX_TIMESTAMP:
            return ProtoValue(int64_val=int(value))
        elif feast_value_type == ValueType.FLOAT:
            return ProtoValue(float_val=float(value))
        elif feast_value_type == ValueType.DOUBLE:
            assert type(value) is float or np.float64
            return ProtoValue(double_val=value)
        elif feast_value_type == ValueType.STRING:
            return ProtoValue(string_val=str(value))
        elif feast_value_type == ValueType.BYTES:
            assert type(value) is bytes
            return ProtoValue(bytes_val=value)
        elif feast_value_type == ValueType.BOOL:
            assert type(value) is bool
            return ProtoValue(bool_val=value)

    raise Exception(f"Unsupported data type: ${str(type(value))}")


def python_value_to_proto_value(
    value: Any, feature_type: ValueType = None
) -> ProtoValue:
    value_type = feature_type
    if value is not None:
        if type(value).__name__ == "ndarray" or isinstance(value, list):
            value_type = (
                feature_type
                if len(value) == 0
                else python_type_to_feast_value_type("", value)
            )
        else:
            value_type = python_type_to_feast_value_type("", value)
    return _python_value_to_proto_value(value_type, value)


def _proto_str_to_value_type(proto_str: str) -> ValueType:
    """
    Returns Feast ValueType given Feast ValueType string.

    Args:
        proto_str: str

    Returns:
        A variant of ValueType.
    """
    type_map = {
        "int32_val": ValueType.INT32,
        "int64_val": ValueType.INT64,
        "double_val": ValueType.DOUBLE,
        "float_val": ValueType.FLOAT,
        "string_val": ValueType.STRING,
        "bytes_val": ValueType.BYTES,
        "bool_val": ValueType.BOOL,
        "int32_list_val": ValueType.INT32_LIST,
        "int64_list_val": ValueType.INT64_LIST,
        "double_list_val": ValueType.DOUBLE_LIST,
        "float_list_val": ValueType.FLOAT_LIST,
        "string_list_val": ValueType.STRING_LIST,
        "bytes_list_val": ValueType.BYTES_LIST,
        "bool_list_val": ValueType.BOOL_LIST,
    }

    return type_map[proto_str]


def pa_to_feast_value_type(pa_type_as_str: str) -> ValueType:
    if re.match(r"^timestamp", pa_type_as_str):
        return ValueType.INT64

    type_map = {
        "int32": ValueType.INT32,
        "int64": ValueType.INT64,
        "double": ValueType.DOUBLE,
        "float": ValueType.FLOAT,
        "string": ValueType.STRING,
        "binary": ValueType.BYTES,
        "bool": ValueType.BOOL,
        "list<item: int32>": ValueType.INT32_LIST,
        "list<item: int64>": ValueType.INT64_LIST,
        "list<item: double>": ValueType.DOUBLE_LIST,
        "list<item: float>": ValueType.FLOAT_LIST,
        "list<item: string>": ValueType.STRING_LIST,
        "list<item: binary>": ValueType.BYTES_LIST,
        "list<item: bool>": ValueType.BOOL_LIST,
    }
    return type_map[pa_type_as_str]


def bq_to_feast_value_type(bq_type_as_str):
    type_map: Dict[ValueType, Union[str, Dict[str, Any]]] = {
        "DATETIME": ValueType.STRING,  # Update to ValueType.UNIX_TIMESTAMP once #1520 lands.
        "TIMESTAMP": ValueType.STRING,  # Update to ValueType.UNIX_TIMESTAMP once #1520 lands.
        "INTEGER": ValueType.INT64,
        "INT64": ValueType.INT64,
        "STRING": ValueType.STRING,
        "FLOAT": ValueType.DOUBLE,
        "FLOAT64": ValueType.DOUBLE,
        "BYTES": ValueType.BYTES,
        "BOOL": ValueType.BOOL,
        "ARRAY<INT64>": ValueType.INT64_LIST,
        "ARRAY<FLOAT64>": ValueType.DOUBLE_LIST,
        "ARRAY<STRING>": ValueType.STRING_LIST,
        "ARRAY<BYTES>": ValueType.BYTES_LIST,
        "ARRAY<BOOL>": ValueType.BOOL_LIST,
    }

    return type_map[bq_type_as_str]


def redshift_to_feast_value_type(redshift_type_as_str: str) -> ValueType:
    # Type names from https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
    type_map = {
        "int2": ValueType.INT32,
        "int4": ValueType.INT32,
        "int8": ValueType.INT64,
        "numeric": ValueType.DOUBLE,
        "float4": ValueType.FLOAT,
        "float8": ValueType.DOUBLE,
        "bool": ValueType.BOOL,
        "character": ValueType.STRING,
        "varchar": ValueType.STRING,
        "timestamp": ValueType.UNIX_TIMESTAMP,
        "timestamptz": ValueType.UNIX_TIMESTAMP,
        # skip date, geometry, hllsketch, time, timetz
    }

    return type_map[redshift_type_as_str.lower()]


def pa_to_redshift_value_type(pa_type_as_str: str) -> str:
    # PyArrow types: https://arrow.apache.org/docs/python/api/datatypes.html
    # Redshift type: https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
    pa_type_as_str = pa_type_as_str.lower()
    if pa_type_as_str.startswith("timestamp"):
        if "tz=" in pa_type_as_str:
            return "timestamptz"
        else:
            return "timestamp"

    if pa_type_as_str.startswith("date"):
        return "date"

    if pa_type_as_str.startswith("decimal"):
        # PyArrow decimal types (e.g. "decimal(38,37)") luckily directly map to the Redshift type.
        return pa_type_as_str

    # We have to take into account how arrow types map to parquet types as well.
    # For example, null type maps to int32 in parquet, so we have to use int4 in Redshift.
    # Other mappings have also been adjusted accordingly.
    type_map = {
        "null": "int4",
        "bool": "bool",
        "int8": "int4",
        "int16": "int4",
        "int32": "int4",
        "int64": "int8",
        "uint8": "int4",
        "uint16": "int4",
        "uint32": "int8",
        "uint64": "int8",
        "float": "float4",
        "double": "float8",
        "binary": "varchar",
        "string": "varchar",
    }

    return type_map[pa_type_as_str]
