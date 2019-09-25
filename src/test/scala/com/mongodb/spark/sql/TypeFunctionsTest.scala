package com.mongodb.spark.sql

import com.mongodb.spark.exceptions.MongoTypeConversionException
import com.mongodb.spark.sql.MapFunctions.documentToRow
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.bson.{BsonDocument, BsonType, BsonValue}

/**
  * convertToDataType string to int
  */
object TypeFunctionsTest {


  def main(args: Array[String]): Unit = {
    println("hello")
    //    val newDocs = Seq(new Document("_id", new ObjectId(objectId)).append("a", 1), new Document("_id", new ObjectId()).append("a", 2))
    val document: BsonDocument = BsonDocument.parse("{name: 'John', age: \"18.02\", height: \"192\"}")
    println(document)
    val bsonValue: BsonValue = document.get("age")
    println(bsonValue)
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = false),
      StructField("age", DoubleType, nullable = false),
      StructField("height", IntegerType, nullable = true)
    ))

    val row: Row = documentToRow(document, schema)
    row.schema.printTreeString()
    println(row.toSeq)

  }

  private def toInt(bsonValue: BsonValue): Int = {
    bsonValue.getBsonType match {
      case BsonType.DECIMAL128 => bsonValue.asDecimal128().decimal128Value().bigDecimalValue().intValue()
      case BsonType.INT32 => bsonValue.asInt32().intValue()
      case BsonType.INT64 => bsonValue.asInt64().intValue()
      case BsonType.DOUBLE => bsonValue.asDouble().intValue()
      case BsonType.STRING => bsonValue.asString().getValue.toInt
      case _ => throw new MongoTypeConversionException(s"Cannot cast ${bsonValue.getBsonType} into a Int")
    }
  }


}
