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
package org.apache.spark.metrics.source

import com.codahale.metrics.MetricRegistry

class RedisSinkMetricSource extends Source {
  override val sourceName: String = "redis_sink"

  override val metricRegistry: MetricRegistry = new MetricRegistry

  val METRIC_TOTAL_ROWS_INSERTED =
    metricRegistry.counter(MetricRegistry.name("feast_ingestion_feature_row_ingested_count"))

  val METRIC_ROWS_LAG =
    metricRegistry.histogram(MetricRegistry.name("feast_ingestion_feature_row_lag_ms"))
}
