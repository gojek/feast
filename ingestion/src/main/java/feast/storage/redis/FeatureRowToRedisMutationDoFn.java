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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Random;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.Duration;
import feast.SerializableCache;
import feast.ingestion.model.Specs;
import feast.ingestion.util.DateUtil;
import feast.options.OptionsParser;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.storage.RedisProto.RedisBucketKey;
import feast.storage.RedisProto.RedisBucketValue;
import feast.storage.redis.RedisCustomIO.Method;
import feast.storage.redis.RedisCustomIO.RedisMutation;
import feast.types.FeatureProto.Feature;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.GranularityProto.Granularity;

public class FeatureRowToRedisMutationDoFn extends DoFn<FeatureRowExtended, RedisMutation> {

  private final SerializableCache<FeatureSpec, RedisFeatureOptions> servingOptionsCache =
      SerializableCache.<FeatureSpec, RedisFeatureOptions>builder()
          .loadingFunction(
              (featureSpec) ->
                  OptionsParser.parse(
                      featureSpec.getDataStores().getServing().getOptionsMap(),
                      RedisFeatureOptions.class))
          .build();
  private Specs specs;
  private Random random;

  public FeatureRowToRedisMutationDoFn(Specs specs) {
    this.specs = specs;
    this.random = new Random();
  }

  // TODO when extracting to a storage module, use reasonable defaults per granularity.
  // TODO check is greater than 1
  static long getBucketId(com.google.protobuf.Timestamp eventTimestamp, Duration bucketSize) {
    checkArgument(bucketSize.getStandardSeconds() > 0, "BucketSize must be greater than zero");
    return eventTimestamp.getSeconds() / bucketSize.getStandardSeconds();
  }

  static RedisBucketKey getRedisBucketKey(
      String entityId, String featureIdSha1Prefix, long bucketId) {
    return RedisBucketKey.newBuilder()
        .setEntityKey(entityId)
        .setFeatureIdSha1Prefix(featureIdSha1Prefix)
        .setBucketId(bucketId)
        .build();
  }

  static String getFeatureIdSha1Prefix(String featureId) {
    return DigestUtils.sha1Hex(featureId.getBytes()).substring(0, 7);
  }

  /** Output a redis mutation object for every feature in the feature row. */
  @ProcessElement
  public void processElement(ProcessContext context) {
    FeatureRowExtended rowExtended = context.element();
    FeatureRow row = rowExtended.getRow();

    String entityKey = row.getEntityKey();

    for (Feature feature : row.getFeaturesList()) {
      String featureId = feature.getId();
      FeatureSpec featureSpec = specs.tryGetFeatureSpec(featureId);
      if (featureSpec == null) {
        continue;
      }
      String featureIdHash = getFeatureIdSha1Prefix(featureId);

      RedisFeatureOptions options = servingOptionsCache.get(featureSpec);

      com.google.protobuf.Timestamp roundedTimestamp =
          DateUtil.roundToGranularity(row.getEventTimestamp(), featureSpec.getGranularity());

      RedisBucketValue value =
          RedisBucketValue.newBuilder()
              .setValue(feature.getValue())
              .setEventTimestamp(roundedTimestamp)
              .build();
      RedisBucketKey keyForLatest = getRedisBucketKey(entityKey, featureIdHash, 0L);

      Duration expiry = options.getExpiryDuration();
      // add randomness to expiry so that it won't expire in the same time.
      long expiryMillis = (long) (expiry.getMillis() * (1 + random.nextFloat()));
      context.output(
          RedisMutation.builder()
              .key(keyForLatest.toByteArray())
              .value(value.toByteArray())
              .expiryMillis(expiryMillis)
              .method(Method.SET)
              .build());

      if (!featureSpec.getGranularity().equals(Granularity.Enum.NONE)) {
        long bucketId = getBucketId(roundedTimestamp, options.getBucketSizeDuration());

        RedisBucketKey keyForValue =
            getRedisBucketKey(entityKey, featureIdHash, roundedTimestamp.getSeconds());

        context.output(
            RedisMutation.builder()
                .key(keyForValue.toByteArray())
                .value(value.toByteArray())
                .expiryMillis(expiryMillis)
                .method(Method.SET)
                .build());

        RedisBucketKey keyForZset = getRedisBucketKey(entityKey, featureIdHash, bucketId);
        context.output(
            RedisMutation.builder()
                .key(keyForZset.toByteArray())
                .value(keyForValue.toByteArray())
                .score(roundedTimestamp.getSeconds())
                .expiryMillis(expiryMillis)
                .method(Method.ZADD)
                .build());
      }
    }
  }
}
