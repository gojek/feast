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
package feast.serving.configuration;

import feast.core.StoreProto.Store.StoreType;
import feast.serving.FeastProperties;
import feast.serving.service.JobService;
import feast.serving.service.NoopJobService;
import feast.serving.service.RedisBackedJobService;
import feast.serving.specs.CachedSpecService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JobServiceConfig {

  @Bean
  public JobService jobService(
      FeastProperties feastProperties,
      CachedSpecService specService,
      StoreConfiguration storeConfiguration) {
    if (!specService.getStore().getType().equals(StoreType.BIGQUERY)) {
      return new NoopJobService();
    }
    StoreType storeType = StoreType.valueOf(feastProperties.getJobs().getStoreType());
    switch (storeType) {
      case REDIS:
        return new RedisBackedJobService(storeConfiguration.getJobStoreRedisConnection());
      case INVALID:
      case BIGQUERY:
      case CASSANDRA:
      case UNRECOGNIZED:
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported store type '%s' for job store", storeType));
    }
  }
}
