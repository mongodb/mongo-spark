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

package com.mongodb.spark.sql.types

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

import org.bson._

private[spark] object BsonCompatibility {

  trait CompatibilityBase[T <: BsonValue] {
    def apply(row: Row): T = {
      Try(fromSparkData(row)) match {
        case Success(bsonValue) => bsonValue
        case Failure(_)         => throw new UnsupportedOperationException(s"Cannot convert data into a BsonValue: $row")
      }
    }
    def apply(bsonValue: T, st: StructType): GenericRowWithSchema = {
      val data = st == structType match {
        case true  => toSparkData(bsonValue)
        case false => toSparkData(bsonValue).reverse
      }
      new GenericRowWithSchema(data, structType)
    }
    val fields: Seq[StructField]
    def structType: StructType = DataTypes.createStructType(fields.toArray)
    def toSparkData(bsonValue: T): Array[Any]
    def fromSparkData(row: Row): T
    def unapply(structType: StructType): Boolean = {
      val otherFields = structType.toSet[StructField]
      val mapper = (x: StructField) => (x.name, x.dataType)
      otherFields.map(mapper) == fields.toSet[StructField].map(mapper)
    }
  }

  object Binary extends CompatibilityBase[BsonBinary] {
    override val fields: Seq[StructField] = Seq(
      DataTypes.createStructField("subType", DataTypes.ByteType, false),
      DataTypes.createStructField("data", DataTypes.BinaryType, true)
    )
    override def toSparkData(bsonValue: BsonBinary): Array[Any] = Array(bsonValue.getType, bsonValue.getData)
    override def fromSparkData(row: Row): BsonBinary = new BsonBinary(row.getByte(0), row.getAs[Array[Byte]](1))
    def getDataType(bsonValue: BsonBinary): DataType = bsonValue.getType match {
      case `defaultType` => DataTypes.BinaryType
      case _             => structType
    }
    private val defaultType = BsonBinarySubType.BINARY.getValue
  }

  object DbPointer extends CompatibilityBase[BsonDbPointer] {
    override val fields: Seq[StructField] = Seq(
      DataTypes.createStructField("ref", DataTypes.StringType, true),
      DataTypes.createStructField("oid", DataTypes.StringType, true)
    )
    override def toSparkData(bsonValue: BsonDbPointer): Array[Any] = Array(bsonValue.getNamespace, bsonValue.getId.toHexString)
    override def fromSparkData(row: Row): BsonDbPointer = new BsonDbPointer(row.getString(0), new org.bson.types.ObjectId(row.getString(1)))
  }

  object JavaScript extends CompatibilityBase[BsonJavaScript] {
    override val fields: Seq[StructField] = Seq(DataTypes.createStructField("code", DataTypes.StringType, true))
    override def toSparkData(bsonValue: BsonJavaScript): Array[Any] = Array(bsonValue.getCode)
    override def fromSparkData(row: Row): BsonJavaScript = new BsonJavaScript(row.getString(0))
  }

  object JavaScriptWithScope extends CompatibilityBase[BsonJavaScriptWithScope] {
    override val fields: Seq[StructField] = Seq(
      DataTypes.createStructField("code", DataTypes.StringType, true),
      DataTypes.createStructField("scope", DataTypes.StringType, true)
    )
    override def toSparkData(bsonValue: BsonJavaScriptWithScope): Array[Any] = Array(bsonValue.getCode, bsonValue.getScope.toJson())
    override def fromSparkData(row: Row): BsonJavaScriptWithScope = new BsonJavaScriptWithScope(row.getString(0), BsonDocument.parse(row.getString(1)))
  }

  object MaxKey extends CompatibilityBase[BsonMaxKey] {
    override val fields: Seq[StructField] = Seq(DataTypes.createStructField("maxKey", DataTypes.IntegerType, false))
    override def toSparkData(bsonValue: BsonMaxKey): Array[Any] = Array(1)
    override def fromSparkData(row: Row): BsonMaxKey = new BsonMaxKey()
  }

  object MinKey extends CompatibilityBase[BsonMinKey] {
    override val fields: Seq[StructField] = Seq(DataTypes.createStructField("minKey", DataTypes.IntegerType, false))
    override def toSparkData(bsonValue: BsonMinKey): Array[Any] = Array(1)
    override def fromSparkData(row: Row): BsonMinKey = new BsonMinKey()
  }

  object ObjectId extends CompatibilityBase[BsonObjectId] {
    override val fields: Seq[StructField] = Seq(DataTypes.createStructField("oid", DataTypes.StringType, true))
    override def toSparkData(bsonValue: BsonObjectId): Array[Any] = Array(bsonValue.getValue.toHexString)
    override def fromSparkData(row: Row): BsonObjectId = new BsonObjectId(new org.bson.types.ObjectId(row.getString(0)))
  }

  object RegularExpression extends CompatibilityBase[BsonRegularExpression] {
    override val fields: Seq[StructField] = Seq(
      DataTypes.createStructField("regex", DataTypes.StringType, true),
      DataTypes.createStructField("options", DataTypes.StringType, true)
    )
    override def toSparkData(bsonValue: BsonRegularExpression): Array[Any] = Array(bsonValue.getPattern, bsonValue.getOptions)
    override def fromSparkData(row: Row): BsonRegularExpression = new BsonRegularExpression(row.getString(0), row.getString(1))
  }

  object Symbol extends CompatibilityBase[BsonSymbol] {
    override val fields: Seq[StructField] = Seq(DataTypes.createStructField("symbol", DataTypes.StringType, true))
    override def toSparkData(bsonValue: BsonSymbol): Array[Any] = Array(bsonValue.getSymbol)
    override def fromSparkData(row: Row): BsonSymbol = new BsonSymbol(row.getString(0))
  }

  object Timestamp extends CompatibilityBase[BsonTimestamp] {
    override val fields: Seq[StructField] = Seq(
      DataTypes.createStructField("time", DataTypes.IntegerType, false),
      DataTypes.createStructField("inc", DataTypes.IntegerType, false)
    )
    override def toSparkData(bsonValue: BsonTimestamp): Array[Any] = Array(bsonValue.getTime, bsonValue.getInc)
    override def fromSparkData(row: Row): BsonTimestamp = new BsonTimestamp(row.getInt(0), row.getInt(1))
  }

  object Undefined extends CompatibilityBase[BsonUndefined] {
    override val fields: Seq[StructField] = Seq(DataTypes.createStructField("undefined", DataTypes.BooleanType, false))
    override def toSparkData(bsonValue: BsonUndefined): Array[Any] = Array(true)
    override def fromSparkData(row: Row): BsonUndefined = new BsonUndefined()
  }

}
