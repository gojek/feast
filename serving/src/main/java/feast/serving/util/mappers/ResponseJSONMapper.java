/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.serving.util.mappers;

<<<<<<< HEAD
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse.FieldValues;
import feast.proto.types.ValueProto.Value;
=======
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse.Record;
import feast.types.ValueProto.Value;
>>>>>>> Update ResponseJSONMapper to use new GetOnlineFeatures protobuf
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// ResponseJSONMapper maps GRPC Response types to more human readable JSON responses
public class ResponseJSONMapper {

  public static List<Map<String, Object>> mapGetOnlineFeaturesResponse(
      GetOnlineFeaturesResponse response) {
    return response.getRecordsList().stream()
        .map(fieldValue -> convertToMap(fieldValue))
        .collect(Collectors.toList());
  }

  private static Map<String, Object> convertToMap(Record record) {
    return record.getFieldsMap().entrySet().stream()
        .collect(Collectors.toMap(es -> es.getKey(), es -> extractValue(es.getValue().getValue())));
  }

  private static Object extractValue(Value value) {
    switch (value.getValCase().getNumber()) {
      case 1:
        return value.getBytesVal();
      case 2:
        return value.getStringVal();
      case 3:
        return value.getInt32Val();
      case 4:
        return value.getInt64Val();
      case 5:
        return value.getDoubleVal();
      case 6:
        return value.getFloatVal();
      case 7:
        return value.getBoolVal();
      case 11:
        return value.getBytesListVal();
      case 12:
        return value.getStringListVal();
      case 13:
        return value.getInt32ListVal();
      case 14:
        return value.getInt64ListVal();
      case 15:
        return value.getDoubleListVal();
      case 16:
        return value.getFloatListVal();
      case 17:
        return value.getBoolListVal();
      default:
        return null;
    }
  }
}
