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

import java.nio.file.{Files, Paths}

import com.google.protobuf.Timestamp
import feast.proto.types.ValueProto

import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest._
import matchers._

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_date

import scala.reflect.runtime.universe.TypeTag
import scala.util.hashing.MurmurHash3

abstract class UnitSpec
    extends AnyFlatSpec
    with should.Matchers
    with OptionValues
    with Inside
    with Inspectors
