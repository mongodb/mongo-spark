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

import java.util.Date

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.Random
import org.apache.spark.sql.types._
import org.bson.Document
import org.bson.types.Binary
import com.mongodb.spark.sql.types.BsonCompatibility
import org.scalacheck.Gen
import org.scalactic.anyvals.{PosInt, PosZInt}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

// scalastyle:off magic.number
trait MongoDataGenerator extends GeneratorDrivenPropertyChecks {
  val _idField: StructField = DataTypes.createStructField("_id", BsonCompatibility.ObjectId.structType, true)
  val sampleSize: Int = 10
  val sampleRatio: Double = 1.0

  val random = new Random()
  val binaryValue = new Binary(Array[Byte](1))
  val minSuccessful = PosInt(5)
  val minSize = PosZInt(10)
  val maxSize = 100

  implicit override val generatorDrivenConfig = PropertyCheckConfiguration(minSuccessful = minSuccessful, minSize = minSize)

  val simpleDataTypes: Seq[DataType] = Seq(DataTypes.NullType, DataTypes.BinaryType, DataTypes.BooleanType, DataTypes.DoubleType,
    DataTypes.IntegerType, DataTypes.LongType, DataTypes.StringType, DataTypes.TimestampType)
  val complexDataTypes: Seq[DataType] = Seq(ArrayTypePlaceholder, StructTypePlaceholder)
  val allDataTypes: Seq[DataType] = simpleDataTypes ++ complexDataTypes

  def subDataType: DataType = Gen.oneOf(simpleDataTypes).sample.get
  def dataSize: Int = Gen.choose(1, maxSize).sample.get
  def createFieldName(prefix: String): String = s"${prefix}_${random.nextInt(10000000)}"
  def fieldSize: Int = random.nextInt(10) + 1
  def maxDepth: Int = random.nextInt(4) + 1

  // Generators
  def genDataType: Gen[DataType] = Gen.oneOf(allDataTypes)
  def genSimpleDataTypes: Gen[Seq[SimpleDataType]] = Gen.zip(simpleDataTypes.map(SimpleDataType))
  def genSimpleDataType: Gen[SimpleDataType] = Gen.delay(Gen.oneOf(genSimpleDataTypes.sample.get))
  def genBoolean: Gen[Boolean] = Gen.oneOf(true, false)
  def genDate: Gen[Date] = Gen.choose(0L, Long.MaxValue).map(new Date(_))
  def genDouble: Gen[Double] = Gen.choose(0.0, 9999.9)
  def genInt: Gen[Int] = Gen.choose(Int.MinValue, Int.MaxValue)
  def genLong: Gen[Long] = Gen.choose(Long.MinValue, Long.MaxValue)
  def genString: Gen[String] = Gen.alphaStr
  def genArrayDataType(): Gen[ArrayDataType] = genArrayDataType(maxDepth)
  def genArrayDataType(maxDepth: Int): Gen[ArrayDataType] = Gen.choose(1, 5).map(x => ArrayDataType(maxDepth, subDataType))
  def genDocumentDataType(): Gen[DocumentDataType] = genDocumentDataType(maxDepth)
  def genDocumentDataType(maxDepth: Int): Gen[DocumentDataType] = Gen.choose(1, 5).map(x => DocumentDataType(maxDepth))
  def getSimpleGenerator(dataType: DataType): Gen[Any] = {
    dataType match {
      case DataTypes.BinaryType    => Gen.oneOf(Seq(binaryValue))
      case DataTypes.BooleanType   => genBoolean
      case DataTypes.DoubleType    => genDouble
      case DataTypes.IntegerType   => genInt
      case DataTypes.LongType      => genLong
      case DataTypes.StringType    => genString
      case DataTypes.TimestampType => genDate
      case _                       => Gen.oneOf(Seq(null)) // scalastyle:ignore
    }
  }

  // DataTypes
  trait MongoDataType {
    lazy val fieldName: String = createFieldName(dataType.typeName)
    lazy val getDocuments: Seq[Document] = values.map(value => new Document(fieldName, value))
    lazy val values: Seq[Any] = (1 to dataSize).map(x => dataGenerator.sample.get)
    lazy val getDocument: Document = new Document(fieldName, value)
    lazy val value: Any = dataGenerator.sample.get
    def dataGenerator: Gen[Any]
    def dataType: DataType
    def field: StructField = DataTypes.createStructField(fieldName, dataType, true)
    def schema: StructType = DataTypes.createStructType(Array(_idField, field))
  }

  case class SimpleDataType(dataType: DataType) extends MongoDataType {
    def dataGenerator: Gen[Any] = getSimpleGenerator(dataType)
  }

  case class ArrayDataType(maxDepth: Int, subDataType: DataType) extends MongoDataType {
    def dataGenerator: Gen[Any] = (1 to dataSize).map(x => getValue(maxDepth)).asJava

    def getValue(depth: Int): Any = {
      @tailrec
      def nestValue(depth: Int, current: Any): Any = {
        if (depth <= 0) current else nestValue(depth - 1, List(current).asJava)
      }
      nestValue(depth, getSimpleGenerator(subDataType).sample.get)
    }

    def dataType: DataType = {
      @tailrec
      def nestValue(depth: Int, current: DataType): DataType = {
        if (depth <= 0) current else nestValue(depth - 1, DataTypes.createArrayType(current, true))
      }
      nestValue(maxDepth, DataTypes.createArrayType(subDataType, true))
    }
  }

  case class DocumentDataType(maxDepth: Int) extends MongoDataType {
    override lazy val getDocuments: Seq[Document] = values.map(_.asInstanceOf[Document])
    override lazy val values: Seq[Any] = (1 to dataSize).map(x => dataGenerator.sample.get)
    override lazy val getDocument: Document = dataGenerator.sample.get.asInstanceOf[Document]

    def dataGenerator: Gen[Any] = Gen.oneOf(Seq(generateDataForStruct(_schema)))
    def dataType: DataType = _schema
    override def schema: StructType = DataTypes.createStructType(_schema.fields.toSeq.+:(_idField).toArray)
    lazy val _schema: StructType = {
      val nextLevel: Int = maxDepth - 1
      val fields: Seq[StructField] = (1 to fieldSize).map(fieldNumber => {
        nextLevel match {
          case x if x <= 1 => genSimpleDataType.sample.get.field
          case _ => genDataType.sample.get match {
            case simple if simpleDataTypes.contains(simple) => SimpleDataType(simple).field
            case array if Seq(ArrayTypePlaceholder).contains(array) => ArrayDataType(nextLevel, genSimpleDataType.sample.get.dataType).field
            case _ => DocumentDataType(nextLevel).field
          }
        }
      }).sortBy(_.name)
      DataTypes.createStructType(fields.toArray)
    }

    def generateDataForStruct(struct: StructType): Document = {
      val document: Document = new Document()
      struct.iterator.toList.map(generateDataForField).foreach(kv => document.append(kv._1, kv._2))
      document
    }

    def generateDataForField(field: StructField): (String, Any) = {
      val fieldName: String = field.name
      val value: Any = field.dataType match {
        case struct: StructType => generateDataForStruct(struct)
        case array: ArrayType => {
          var subDataType = array.elementType
          var depth = 0
          while (subDataType.isInstanceOf[ArrayType]) {
            depth += 1
            subDataType = subDataType.asInstanceOf[ArrayType].elementType
          }
          ArrayDataType(depth, subDataType).value
        }
        case x => getSimpleGenerator(x).sample.get
      }
      (fieldName, value)
    }
  }

  // Placeholders for complex types which are inaccessible outside spark
  case object ArrayTypePlaceholder extends DataType {
    override def typeName: String = "array"
    def defaultSize: Int = 0
    def asNullable: DataType = this
  }

  case object StructTypePlaceholder extends DataType {
    override def typeName: String = "struct"
    def defaultSize: Int = 0
    def asNullable: DataType = this
  }

}
// scalastyle:on magic.number
