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

import java.io.{StringWriter, Writer}

import org.bson.codecs.{BsonValueCodec, EncoderContext}
import org.bson.json.{JsonWriter, JsonWriterSettings}
import org.bson.{AbstractBsonWriter, BsonValue}

private[sql] object BsonValueToJson {
  val codec = new BsonValueCodec()
  def apply(element: BsonValue): String = {
    val writer: StringWriter = new StringWriter
    new BsonValueCodec().encode(new ValueStateJsonWriter(writer), element, EncoderContext.builder.build)
    writer.toString
  }

  case class ValueStateJsonWriter(
    writer:   Writer,
    settings: JsonWriterSettings = new JsonWriterSettings()
  ) extends JsonWriter(
    writer: Writer,
    settings: JsonWriterSettings
  ) {
    setState(AbstractBsonWriter.State.VALUE)
  }

}

