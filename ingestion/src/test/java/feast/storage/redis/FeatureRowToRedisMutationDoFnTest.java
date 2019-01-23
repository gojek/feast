/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.storage.redis;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import feast.ingestion.model.Specs;
import feast.ingestion.util.DateUtil;
import feast.storage.RedisProto.RedisBucketKey;
import feast.storage.redis.RedisCustomIO.RedisMutation;
import feast.types.FeatureProto.Feature;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class FeatureRowToRedisMutationDoFnTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  Specs specs;
  FeatureRowToRedisMutationDoFn doFn;

  @Before
  public void setUp() throws Exception {
    specs = mock(Specs.class, withSettings().serializable());
    doFn = new FeatureRowToRedisMutationDoFn(specs);
  }

  @Test
  public void testRedisBucketKeySize() {
    Duration bucketSize = Duration.standardMinutes(20);
    DateTime dt = new DateTime(2018, 6, 22, 4, 15, 26);
    int bucketId =
        Math.toIntExact(DateUtil.toTimestamp(dt).getSeconds() / bucketSize.getStandardSeconds());
    String featureIdHash = DigestUtils.sha1Hex("driver.seconds.pings_v1").substring(0, 7);
    RedisBucketKey key =
        RedisBucketKey.newBuilder()
            .setEntityKey(String.valueOf(12345678))
            .setFeatureIdSha1Prefix(featureIdHash)
            .setBucketId(bucketId)
            .build();
    System.out.println(key.toByteArray().length);
  }

  @Test
  public void shouldIgnoreUnknownFeatureId() {
    FeatureRow row =
        FeatureRow.newBuilder()
            .setEntityKey("1234")
            .setEntityName("testEntity")
            // this feature should be ignored
            .addFeatures(Feature.newBuilder().setId("testEntity.none.unknown_feature"))
            .build();
    FeatureRowExtended rowExtended = FeatureRowExtended.newBuilder().setRow(row).build();
    PCollection<FeatureRowExtended> p = pipeline.apply(Create.of(rowExtended));
    PCollection<RedisMutation> out = p.apply(ParDo.of(doFn));
    PAssert.that(out).empty();

    pipeline.run();
  }
}
