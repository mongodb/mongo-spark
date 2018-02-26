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

import java.util.regex.Pattern

import scala.collection.JavaConverters._

import org.apache.spark.sql.sources._

import org.bson.conversions.Bson
import com.mongodb.client.model.{Aggregates, Filters, Projections}

private[spark] object MongoRelationHelper {

  def createPipeline(requiredColumns: Array[String], filters: Array[Filter]): Seq[Bson] = {
    var pipeline: List[Bson] = List()
    if (requiredColumns.nonEmpty) pipeline = Aggregates.project(createProjection(requiredColumns)) :: pipeline
    if (filters.nonEmpty) pipeline = Aggregates.`match`(createMatch(filters)) :: pipeline
    pipeline
  }

  // scalastyle:off cyclomatic.complexity null
  private def createMatch(filters: Array[Filter]): Bson = {
    val matchPipelineStage: Array[Bson] = filters.map {
      case EqualTo(field, value)            => Filters.eq(field, value)
      case EqualNullSafe(field, value)      => Filters.eq(field, value)
      case GreaterThan(field, value)        => Filters.gt(field, value)
      case GreaterThanOrEqual(field, value) => Filters.gte(field, value)
      case In(field, values)                => Filters.in(field, values.toList.asJava)
      case LessThan(field, value)           => Filters.lt(field, value)
      case LessThanOrEqual(field, value)    => Filters.lte(field, value)
      case IsNull(field)                    => Filters.eq(field, null)
      case IsNotNull(field)                 => Filters.and(Filters.exists(field), Filters.ne(field, null))
      case And(leftFilter, rightFilter)     => Filters.and(createMatch(Array(leftFilter)), createMatch(Array(rightFilter)))
      case Or(leftFilter, rightFilter)      => Filters.or(createMatch(Array(leftFilter)), createMatch(Array(rightFilter)))
      case Not(filter)                      => Filters.not(createMatch(Array(filter)))
      case StringStartsWith(field, value)   => Filters.regex(field, Pattern.compile("^" + value))
      case StringEndsWith(field, value)     => Filters.regex(field, Pattern.compile(value + "$"))
      case StringContains(field, value)     => Filters.regex(field, Pattern.compile(value))
    }
    Filters.and(matchPipelineStage: _*)
  }
  // scalastyle:on cyclomatic.complexity null

  private def createProjection(requiredColumns: Array[String]): Bson = {
    requiredColumns.contains("_id") match {
      case true  => Projections.include(requiredColumns: _*)
      case false => Filters.and(Projections.include(requiredColumns: _*), Projections.excludeId())
    }
  }

}
