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

import java.util

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion
import org.apache.spark.sql.catalyst.{JavaTypeInference, ScalaReflection}
import org.apache.spark.sql.types._

import org.bson._
import com.mongodb.client.model.{Aggregates, Filters, Projections, Sorts}
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark.sql.types.{ConflictType, BsonCompatibility, SkipFieldType}

object MongoInferSchema {

  /**
   * Gets a schema for the specified mongo collection. It is required that the
   * collection provides Documents.
   *
   * Utilizes the `\$sample` aggregation operator in server versions 3.2+. Older versions take a sample of the most recent 10k documents.
   *
   * @param sc                 the spark context
   * @return the schema for the collection
   */
  def apply(sc: SparkContext): StructType = apply(MongoRDD[BsonDocument](sc))

  /**
   * Gets a schema for the specified mongo collection. It is required that the
   * collection provides Documents.
   *
   * Utilizes the `\$sample` aggregation operator in server versions 3.2+. Older versions take a sample of the most recent 10k documents.
   *
   * @param mongoRDD           the MongoRDD to be sampled
   * @return the schema for the collection
   */
  def apply(mongoRDD: MongoRDD[BsonDocument]): StructType = {
    val sampleData: MongoRDD[BsonDocument] = mongoRDD.hasSampleAggregateOperator match {
      case true => mongoRDD.appendPipeline(Seq(Aggregates.sample(mongoRDD.readConfig.sampleSize)))
      case false =>
        val samplePool: Int = 10000
        val sampleSize: Int = if (mongoRDD.readConfig.sampleSize > samplePool) samplePool else mongoRDD.readConfig.sampleSize
        val sampleData: Seq[BsonDocument] = mongoRDD.appendPipeline(Seq(
          Aggregates.project(Projections.include("_id")), Aggregates.sort(Sorts.descending("_id")), Aggregates.limit(samplePool)
        )).takeSample(withReplacement = false, num = sampleSize).toSeq
        Try(sampleData.map(_.get("_id")).asJava) match {
          case Success(_ids) => mongoRDD.appendPipeline(Seq(Aggregates.`match`(Filters.in("_id", _ids))))
          case Failure(_) =>
            throw new IllegalArgumentException("The RDD must contain documents that include an '_id' key to infer data when using MongoDB < 3.2")
        }
    }
    // perform schema inference on each row and merge afterwards
    val rootType: DataType = sampleData.mapPartitions(_.map(doc => getSchemaFromDocument(doc)))
      .treeAggregate[DataType](StructType(Seq()))(compatibleType, compatibleType)

    canonicalizeType(rootType) match {
      case Some(st: StructType) => st
      case _                    => StructType(Seq()) // canonicalizeType erases all empty structs, including the only one we want to keep
    }
  }

  /**
   * Remove StructTypes with no fields or SkipFields
   */
  private def canonicalizeType: DataType => Option[DataType] = {
    case at @ ArrayType(elementType, _) =>
      for {
        canonicalType <- canonicalizeType(elementType)
      } yield {
        at.copy(canonicalType)
      }

    case StructType(fields) =>
      val canonicalFields = for {
        field <- fields
        if field.name.nonEmpty
        if field.dataType != SkipFieldType
        canonicalType <- canonicalizeType(field.dataType)
      } yield {
        field.copy(dataType = canonicalType)
      }

      if (canonicalFields.nonEmpty) {
        Some(StructType(canonicalFields))
      } else {
        // per SPARK-8093: empty structs should be deleted
        None
      }
    case other => Some(other)
  }

  private def getSchemaFromDocument(document: BsonDocument): StructType = {
    val fields = new util.ArrayList[StructField]()
    document.entrySet.asScala.foreach(kv => fields.add(DataTypes.createStructField(kv.getKey, getDataType(kv.getValue), true)))
    DataTypes.createStructType(fields)
  }

  /**
   * Gets the matching DataType for the input DataTypes.
   *
   * For simple types, returns a ConflictType if the DataTypes do not match.
   *
   * For complex types:
   * - ArrayTypes: if the DataTypes of the elements cannot be matched, then
   * an ArrayType(ConflictType, true) is returned.
   * - StructTypes: for any field on which the DataTypes conflict, the field
   * value is replaced with a ConflictType.
   *
   * @param t1 the DataType of the first element
   * @param t2 the DataType of the second element
   * @return the DataType that matches on the input DataTypes
   */
  private def compatibleType(t1: DataType, t2: DataType): DataType = {
    HiveTypeCoercion.findTightestCommonTypeOfTwo(t1, t2).getOrElse {
      // t1 or t2 is a StructType, ArrayType, or an unexpected type.
      (t1, t2) match {
        case (StructType(fields1), StructType(fields2)) =>
          val newFields = (fields1 ++ fields2).groupBy(field => field.name).map {
            case (name, fieldTypes) =>
              val dataType = fieldTypes.view.map(_.dataType).reduce(compatibleType)
              StructField(name, dataType, nullable = true)
          }
          StructType(newFields.toSeq.sortBy(_.name))
        case (ArrayType(elementType1, containsNull1), ArrayType(elementType2, containsNull2)) =>
          ArrayType(compatibleType(elementType1, elementType2), containsNull1 || containsNull2)
        // SkipFieldType Types
        case (s: SkipFieldType, dataType: DataType) => dataType
        case (dataType: DataType, s: SkipFieldType) => dataType
        // Conflicting Types
        case (_, _)                                 => ConflictType
      }
    }
  }

  // scalastyle:off cyclomatic.complexity null
  private def getDataType(bsonValue: BsonValue): DataType = {
    bsonValue.getBsonType match {
      case BsonType.NULL                  => DataTypes.NullType
      case BsonType.ARRAY                 => getSchemaFromArray(bsonValue.asArray().asScala)
      case BsonType.BINARY                => BsonCompatibility.Binary.getDataType(bsonValue.asBinary())
      case BsonType.BOOLEAN               => DataTypes.BooleanType
      case BsonType.DATE_TIME             => DataTypes.TimestampType
      case BsonType.DOCUMENT              => getSchemaFromDocument(bsonValue.asDocument())
      case BsonType.DOUBLE                => DataTypes.DoubleType
      case BsonType.INT32                 => DataTypes.IntegerType
      case BsonType.INT64                 => DataTypes.LongType
      case BsonType.STRING                => DataTypes.StringType
      case BsonType.OBJECT_ID             => BsonCompatibility.ObjectId.structType
      case BsonType.TIMESTAMP             => BsonCompatibility.Timestamp.structType
      case BsonType.MIN_KEY               => BsonCompatibility.MinKey.structType
      case BsonType.MAX_KEY               => BsonCompatibility.MaxKey.structType
      case BsonType.JAVASCRIPT            => BsonCompatibility.JavaScript.structType
      case BsonType.JAVASCRIPT_WITH_SCOPE => BsonCompatibility.JavaScriptWithScope.structType
      case BsonType.REGULAR_EXPRESSION    => BsonCompatibility.RegularExpression.structType
      case BsonType.UNDEFINED             => BsonCompatibility.Undefined.structType
      case BsonType.SYMBOL                => BsonCompatibility.Symbol.structType
      case BsonType.DB_POINTER            => BsonCompatibility.DbPointer.structType
      case _                              => ConflictType
    }
  }

  private def getSchemaFromArray(bsonArray: Seq[BsonValue]): DataType = {
    val arrayTypes: Seq[BsonType] = bsonArray.map(_.getBsonType).distinct
    arrayTypes.length match {
      case 0 => SkipFieldType
      case 1 if Seq(BsonType.ARRAY, BsonType.DOCUMENT).contains(arrayTypes.head) => getCompatibleArraySchema(bsonArray)
      case 1 => DataTypes.createArrayType(getDataType(bsonArray.head), true)
      case _ => getCompatibleArraySchema(bsonArray)
    }
  }
  // scalastyle:on cyclomatic.complexity null

  def getCompatibleArraySchema(bsonArray: Seq[BsonValue]): DataType = {
    var arrayType: Option[DataType] = Some(SkipFieldType)
    bsonArray.takeWhile({
      case (bsonValue: BsonNull) => true
      case (bsonValue: BsonValue) =>
        val previous: Option[DataType] = arrayType
        arrayType = Some(getDataType(bsonValue))
        if (previous.nonEmpty && arrayType != previous) arrayType = Some(compatibleType(arrayType.get, previous.get))
        arrayType != Some(ConflictType) // Option.contains was added in Scala 2.11
    })
    arrayType.get match {
      case SkipFieldType => SkipFieldType
      case ConflictType  => ConflictType
      case dataType      => DataTypes.createArrayType(dataType, true)
    }
  }

  def reflectSchema[T <: Product: TypeTag](): Option[StructType] = {
    typeOf[T] match {
      case x if x == typeOf[Nothing] => None
      case _                         => Some(ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType])
    }
  }

  def reflectSchema[T](beanClass: Class[T]): StructType = JavaTypeInference.inferDataType(beanClass)._1.asInstanceOf[StructType]

}
