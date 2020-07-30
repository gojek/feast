/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
package feast.common.logging.entry;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import feast.common.logging.entry.LogResource.ResourceType;
import feast.proto.serving.ServingAPIProto.FeatureReference;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse.FieldValues;
import feast.proto.types.ValueProto.Value;
import io.grpc.Status;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class AuditLogEntryTest {
  public List<AuditLogEntry> getTestAuditLogs() {
    GetOnlineFeaturesRequest requestSpec =
        GetOnlineFeaturesRequest.newBuilder()
            .setOmitEntitiesInResponse(false)
            .addAllFeatures(
                Arrays.asList(
                    FeatureReference.newBuilder().setName("feature1").build(),
                    FeatureReference.newBuilder().setName("feature2").build()))
            .build();

    GetOnlineFeaturesResponse responseSpec =
        GetOnlineFeaturesResponse.newBuilder()
            .addAllFieldValues(
                Arrays.asList(
                    FieldValues.newBuilder()
                        .putFields("feature", Value.newBuilder().setInt32Val(32).build())
                        .build(),
                    FieldValues.newBuilder()
                        .putFields("feature2", Value.newBuilder().setInt32Val(64).build())
                        .build()))
            .build();

    return Arrays.asList(
        MessageAuditLogEntry.newBuilder()
            .setComponent("feast-serving")
            .setVersion("0.6")
            .setService("ServingService")
            .setMethod("getOnlineFeatures")
            .setRequest(requestSpec)
            .setResponse(responseSpec)
            .setStatusCode(Status.OK.getCode())
            .setIdentity("adam@no.such.email")
            .build(),
        ActionAuditLogEntry.of(
            "core", "0.6", LogResource.of(ResourceType.JOB, "kafka-to-redis"), "CREATE"),
        TransitionAuditLogEntry.of(
            "core",
            "0.6",
            LogResource.of(ResourceType.FEATURE_SET, "project/feature_set"),
            "READY"));
  }

  @Test
  public void shouldReturnJSONRepresentationOfAuditLog() {
    for (AuditLogEntry auditLog : getTestAuditLogs()) {
      // Check that auditLog's toJSON() returns valid JSON
      String logJSON = auditLog.toJSON();
      System.out.println(logJSON);
      JsonParser parser = new JsonParser();

      // check basic fields are present in JSON representation.
      JsonObject logObject = parser.parse(logJSON).getAsJsonObject();
      assertThat(logObject.getAsJsonPrimitive("logType").getAsString(), equalTo(auditLog.logType));
      assertThat(
          logObject.getAsJsonPrimitive("kind").getAsString(), equalTo(auditLog.getKind().name()));
    }
  }
}
