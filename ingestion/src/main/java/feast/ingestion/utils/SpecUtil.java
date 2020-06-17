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
package feast.ingestion.utils;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.ingestion.values.Field;
import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.core.IngestionJobProto;
import feast.proto.core.IngestionJobProto.SpecsStreamingUpdateConfig;
import feast.proto.core.SourceProto.Source;
import feast.proto.core.StoreProto.Store;
import feast.proto.core.StoreProto.Store.Subscription;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.lang3.tuple.Pair;

public class SpecUtil {
  public static String PROJECT_DEFAULT_NAME = "default";

  public static String getFeatureSetReference(FeatureSetSpec featureSetSpec) {
    return String.format("%s/%s", featureSetSpec.getProject(), featureSetSpec.getName());
  }

  public static Pair<String, String> parseFeatureSetReference(String reference) {
    String[] split = reference.split("/", 2);
    if (split.length == 1) {
      return Pair.of(PROJECT_DEFAULT_NAME, split[0]);
    } else {
      return Pair.of(split[0], split[1]);
    }
  }

  /** Get only feature set specs that matches the subscription */
  public static boolean isSubscribedToFeatureSet(
      List<Subscription> subscriptions, String projectName, String featureSetName) {

    for (Subscription sub : subscriptions) {
      // If configuration missing, fail
      if (sub.getProject().isEmpty() || sub.getName().isEmpty()) {
        throw new IllegalArgumentException(
            String.format("Subscription is missing arguments: %s", sub.toString()));
      }

      // If all wildcards, subscribe to everything
      if (sub.getProject().equals("*") || sub.getName().equals("*")) {
        return true;
      }

      // Match project name
      if (!projectName.equals(sub.getProject())) {
        continue;
      }

      // Convert wildcard to regex
      String subName = sub.getName();
      if (!sub.getName().contains(".*")) {
        subName = subName.replace("*", ".*");
      }

      // Match feature set name to pattern
      Pattern pattern = Pattern.compile(subName);
      if (!pattern.matcher(featureSetName).matches()) {
        continue;
      }
      return true;
    }

    return false;
  }

  public static List<Store> parseStoreJsonList(List<String> jsonList)
      throws InvalidProtocolBufferException {
    List<Store> stores = new ArrayList<>();
    for (String json : jsonList) {
      Store.Builder builder = Store.newBuilder();
      JsonFormat.parser().merge(json, builder);
      stores.add(builder.build());
    }
    return stores;
  }

  public static Source parseSourceJson(String jsonSource) throws InvalidProtocolBufferException {
    Source.Builder builder = Source.newBuilder();
    JsonFormat.parser().merge(jsonSource, builder);
    return builder.build();
  }

  public static IngestionJobProto.SpecsStreamingUpdateConfig parseSpecsStreamingUpdateConfig(
      String jsonConfig) throws InvalidProtocolBufferException {
    SpecsStreamingUpdateConfig.Builder builder = SpecsStreamingUpdateConfig.newBuilder();
    JsonFormat.parser().merge(jsonConfig, builder);
    return builder.build();
  }

  public static Map<String, Field> getFieldsByName(FeatureSetSpec featureSetSpec) {
    Map<String, Field> fieldByName = new HashMap<>();
    for (EntitySpec entitySpec : featureSetSpec.getEntitiesList()) {
      fieldByName.put(
          entitySpec.getName(), new Field(entitySpec.getName(), entitySpec.getValueType()));
    }
    for (FeatureSpec featureSpec : featureSetSpec.getFeaturesList()) {
      fieldByName.put(
          featureSpec.getName(), new Field(featureSpec.getName(), featureSpec.getValueType()));
    }
    return fieldByName;
  }
}
