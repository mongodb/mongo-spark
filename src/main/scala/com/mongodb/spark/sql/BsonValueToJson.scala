/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.spark.sql

import java.io.StringWriter
import java.lang

import org.bson.{BsonBinary, BsonRegularExpression, BsonValue}
import org.bson.codecs.{BsonValueCodec, EncoderContext}
import org.bson.internal.Base64
import org.bson.json.{JsonWriter, JsonWriterSettings, StrictJsonWriter, JsonMode, Converter}

private[sql] object BsonValueToJson {
  val codec = new BsonValueCodec()

  def apply(element: BsonValue): String = {
    val stringWriter: StringWriter = new StringWriter
    val jsonWriter = new JsonWriter(stringWriter, JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).indent(false)
      .dateTimeConverter(new Converter[lang.Long] {
        override def convert(value: lang.Long, writer: StrictJsonWriter): Unit = {
          writer.writeStartObject()
          writer.writeNumber("$date", value.toLong.toString)
          writer.writeEndObject()
        }
      })
      .int32Converter(new Converter[Integer] {
        override def convert(value: Integer, writer: StrictJsonWriter): Unit = {
          writer.writeNumber(value.toString)
        }
      })
      .regularExpressionConverter(new Converter[BsonRegularExpression] {
        override def convert(value: BsonRegularExpression, writer: StrictJsonWriter): Unit = {
          writer.writeStartObject()
          writer.writeString("$regex", value.getPattern)
          writer.writeString("$options", value.getOptions)
          writer.writeEndObject()
        }
      })
      .binaryConverter(new Converter[BsonBinary] {
        override def convert(value: BsonBinary, writer: StrictJsonWriter): Unit = {
          writer.writeStartObject()
          writer.writeString("$binary", Base64.encode(value.getData))
          writer.writeString("$type", f"${value.getType}%02X")
          writer.writeEndObject()
        }
      })
      .build())

    jsonWriter.writeStartDocument()
    jsonWriter.writeName("k")
    codec.encode(jsonWriter, element, EncoderContext.builder.build)
    stringWriter.getBuffer.toString.split(":", 2)(1).trim
  }
}

