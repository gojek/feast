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
package feast.ingestion

import feast.ingestion.sources.bq.BigQueryReader
import feast.ingestion.sources.file.FileReader
import feast.ingestion.validation.RowValidator
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.col

object OfflinePipeline extends BasePipeline {
  override def createPipeline(sparkSession: SparkSession, config: IngestionJobConfig): Unit = {
    val featureTable = config.featureTable
    val projection =
      inputProjection(featureTable.offline_source.get, featureTable.features, featureTable.entities)
    val validator = new RowValidator(featureTable)

    val input = config.featureTable.offline_source match {
      case Some(source: BQSource) =>
        BigQueryReader.createBatchSource(
          sparkSession.sqlContext,
          source,
          config.startTime,
          config.endTime
        )
      case Some(source: GSSource) =>
        FileReader.createBatchSource(
          sparkSession.sqlContext,
          source,
          config.startTime,
          config.endTime
        )
    }

    val projected = input.select(projection: _*).cache()

    val validRows = projected
      .filter(validator.checkAll)

    validRows.write
      .format("feast.ingestion.stores.redis")
      .option("entity_columns", featureTable.entities.map(_.name).mkString(","))
      .option("namespace", featureTable.name)
      .option("project_name", featureTable.project)
      .option("timestamp_column", featureTable.offline_source.get.timestampColumn)
      .save()

    config.deadLetterPath match {
      case Some(path) =>
        projected
          .filter(!validator.checkAll)
          .write
          .format("parquet")
          .save(path)
      case _ => None
    }

  }

  private def inputProjection(
      source: OfflineSource,
      features: Seq[Field],
      entities: Seq[Field]
  ): Array[Column] = {
    val featureColumns =
      if (source.mapping.nonEmpty)
        source.mapping
      else features.map(f => (f.name, f.name))

    val timestampColumn = Seq((source.timestampColumn, source.timestampColumn))
    val entitiesColumns =
      entities.filter(e => !source.mapping.contains(e.name)).map(e => (e.name, e.name))

    (featureColumns ++ entitiesColumns ++ timestampColumn).map { case (alias, source) =>
      col(source).alias(alias)
    }.toArray
  }
}
