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

import java.sql.Timestamp
import java.util.Date
import java.util.regex.Pattern

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import org.bson.conversions.Bson
import org.bson.types.ObjectId
import org.bson.{BsonTimestamp, Document}
import com.mongodb.client.model.{Aggregates, Filters, Projections}

private[sql] object MongoRelationHelper {

  def documentToRow(document: Document, schema: StructType, requiredColumns: Array[String] = Array.empty[String]): Row = {
    val values: Array[(Any, StructField)] = schema.fields.map(field =>
      document.containsKey(field.name) match {
        case true  => (convert(document.get(field.name), field.dataType), field)
        case false => (null, field) // scalastyle:ignore
      })

    val requiredValues = requiredColumns.nonEmpty match {
      case true  => values.collect({ case (rowValue, rowField) if requiredColumns.contains(rowField.name) => (rowValue, rowField) })
      case false => values
    }
    new GenericRowWithSchema(requiredValues.map(_._1), DataTypes.createStructType(requiredValues.map(_._2)))
  }

  def rowToDocument(row: Row): Document = {
    val document = new Document()
    row.schema.fields.zipWithIndex.foreach({
      case (field, i) =>
        val data = field.dataType match {
          case arrayField: ArrayType   => arrayTypeToData(arrayField, row.getSeq(i))
          case subDocument: StructType => rowToDocument(row.getStruct(i))
          case _                       => row.get(i)
        }
        document.append(field.name, data)
    })
    document
  }

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
      case IsNotNull(field)                 => Filters.ne(field, null)
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

  private def convert(element: Any, elementType: DataType): Any = {
    // TODO - refer to how spark handles errors with Json data
    Try(castToDataType(element, elementType)) match {
      case Success(value) => value
      case Failure(ex)    => throw new RuntimeException(s"Could not convert $element to ${elementType.typeName}")
    }
  }

  private def castToDataType(element: Any, elementType: DataType): Any = {
    elementType match {
      case _: TimestampType => new Timestamp(element.asInstanceOf[BsonTimestamp].getTime * 1000L)
      case _: DateType      => new Date(element.asInstanceOf[Date].getTime)
      case _: ArrayType =>
        val innerElementType: DataType = elementType.asInstanceOf[ArrayType].elementType
        element.asInstanceOf[java.util.List[_]].asScala.map(innerElement => convert(innerElement, innerElementType))
      case schema: StructType => documentToRow(element.asInstanceOf[Document], schema)
      case _: StringType if element.isInstanceOf[ObjectId] => element.asInstanceOf[ObjectId].toHexString
      case _ => element
    }
  }

  private def arrayTypeToData(arrayField: ArrayType, data: Seq[Any]): Any = {
    arrayField.elementType match {
      case subDocuments: StructType => data.map(x => rowToDocument(x.asInstanceOf[Row])).asJava
      case subArray: ArrayType      => data.map(x => elementTypeToData(subArray.elementType, x.asInstanceOf[Seq[Any]])).asJava
      case _                        => data.asJava
    }
  }

  private def elementTypeToData(dataType: DataType, data: Seq[Any]): Any = {
    dataType match {
      case arrayField: ArrayType   => arrayTypeToData(arrayField, data)
      case subDocument: StructType => data.map(x => rowToDocument(x.asInstanceOf[Row])).asJava
      case _                       => data
    }
  }
}
