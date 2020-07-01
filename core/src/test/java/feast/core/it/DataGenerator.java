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
package feast.core.it;

import com.google.common.collect.ImmutableList;
import feast.proto.core.SourceProto;
import feast.proto.core.StoreProto;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Triple;

public class DataGenerator {
  // projectName, featureName, exclude
  public static Triple<String, String, Boolean> defaultSubscription = Triple.of("*", "*", false);

  public static StoreProto.Store defaultStore =
      createStore(
          "test-store", StoreProto.Store.StoreType.REDIS, ImmutableList.of(defaultSubscription));

  public static SourceProto.Source defaultSource = createSource("localhost", "topic");

  public static SourceProto.Source createSource(String server, String topic) {
    return SourceProto.Source.newBuilder()
        .setType(SourceProto.SourceType.KAFKA)
        .setKafkaSourceConfig(
            SourceProto.KafkaSourceConfig.newBuilder()
                .setBootstrapServers(server)
                .setTopic(topic)
                .build())
        .build();
  }

  public static StoreProto.Store createStore(
      String name,
      StoreProto.Store.StoreType type,
      List<Triple<String, String, Boolean>> subscriptions) {
    return StoreProto.Store.newBuilder()
        .addAllSubscriptions(
            subscriptions.stream()
                .map(
                    s ->
                        StoreProto.Store.Subscription.newBuilder()
                            .setProject(s.getLeft())
                            .setName(s.getMiddle())
                            .setExclude(s.getRight())
                            .build())
                .collect(Collectors.toList()))
        .setName(name)
        .setType(type)
        .build();
  }
}
