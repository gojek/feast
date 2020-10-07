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
package feast.ingestion.utils

import java.sql

import com.google.protobuf.Descriptors.{EnumValueDescriptor, FieldDescriptor}
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._
import com.google.protobuf.{AbstractMessage, ByteString, GeneratedMessageV3, Timestamp}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import collection.convert.ImplicitConversions._

object ProtoReflection {
  def structFieldFor(fd: FieldDescriptor): Option[StructField] = {
    val dataType = fd.getJavaType match {
      case INT         => Some(IntegerType)
      case LONG        => Some(LongType)
      case FLOAT       => Some(FloatType)
      case DOUBLE      => Some(DoubleType)
      case BOOLEAN     => Some(BooleanType)
      case STRING      => Some(StringType)
      case BYTE_STRING => Some(BinaryType)
      case ENUM        => Some(StringType)
      case MESSAGE =>
        fd.getMessageType.getFullName match {
          case "google.protobuf.Timestamp" => Some(TimestampType)
          case _ =>
            Option(fd.getMessageType.getFields.flatMap(structFieldFor))
              .filter(_.nonEmpty)
              .map(StructType.apply)
        }
    }

    dataType.map(dt =>
      StructField(
        fd.getName,
        if (fd.isRepeated) ArrayType(dt, containsNull = false) else dt,
        nullable = !fd.isRequired && !fd.isRepeated
      )
    )
  }

  /**
    * @param defaultInstance except protobuf message instance because it's serializable
    *                        and both parser & fields descriptor can be extracted from it
    */
  def createMessageParser(defaultInstance: GeneratedMessageV3): Array[Byte] => Row = {
    def toRowData(fd: FieldDescriptor, obj: AnyRef): AnyRef = {
      fd.getJavaType match {
        case BYTE_STRING => obj.asInstanceOf[ByteString].toByteArray
        case ENUM        => obj.asInstanceOf[EnumValueDescriptor].getName
        case MESSAGE =>
          fd.getMessageType.getFullName match {
            case "google.protobuf.Timestamp" =>
              new sql.Timestamp(obj.asInstanceOf[Timestamp].getSeconds * 1000)
            case _ => messageToRow(obj.asInstanceOf[AbstractMessage])
          }

        case _ => obj
      }
    }

    def defaultValue(fd: FieldDescriptor): AnyRef = {
      fd.getJavaType match {
        case ENUM    => null
        case MESSAGE => null
        case _       => fd.getDefaultValue
      }
    }

    def messageToRow(message: AbstractMessage) = {
      val fields = message.getAllFields

      Row(defaultInstance.getDescriptorForType.getFields.map { fd =>
        if (fields.containsKey(fd)) {
          val obj = fields.get(fd)
          if (fd.isRepeated) {
            obj.asInstanceOf[java.util.List[Object]].map(toRowData(fd, _))
          } else {
            toRowData(fd, obj)
          }
        } else if (fd.isRepeated) {
          Seq()
        } else defaultValue(fd)
      }: _*)
    }

    bytes: Array[Byte] =>
      messageToRow(defaultInstance.getParserForType.parseFrom(bytes).asInstanceOf[AbstractMessage])
  }
}
