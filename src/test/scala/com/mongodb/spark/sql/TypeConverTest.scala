package com.mongodb.spark.sql

import com.mongodb.spark.sql.MapFunctions.documentToRow
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, _}
import org.bson.{BsonDocument, BsonValue}
import org.apache.spark.sql.types.{ArrayType, NullType, StringType, TimestampType, _}

object TypeConverTest {

  def main(args: Array[String]): Unit = {
    println("hello")
    //    val newDocs = Seq(new Document("_id", new ObjectId(objectId)).append("a", 1), new Document("_id", new ObjectId()).append("a", 2))
    val document: BsonDocument = BsonDocument.
      parse(
        """{name: 'John', age: "", height: "192",sfb: 'true', money:'25.5',arrcol:[{k1:'a',k2:10}],
          |mtest:{k1:'a',k2:'b'}}""".stripMargin)
    println(document)
    val bsonValue: BsonValue = document.get("age")
    println(bsonValue)
//    StructField("k1", StructType(Seq(
//      StructField("is_large_animal", BooleanType, true),
//      StructField("is_mammal", BooleanType, true)
//    )))
    //定义json的Schema
    val arrayStruct = ArrayType(StructType(Seq(
      StructField("k1", StringType, true),
        StructField("k2", DoubleType, true))
    ), true)

    val structType = StructType(Seq(
      StructField("k1", StringType, true),
      StructField("k2", DoubleType, true))
    )

    val schema = StructType(Seq(
      StructField("name", StringType, nullable = false),
      StructField("age", DoubleType, nullable = false),
      StructField("height", IntegerType, nullable = true),
      StructField("sfb", BooleanType, nullable = true),
      StructField("money", DoubleType, nullable = true),
     StructField("arrcol", arrayStruct),
      StructField("mtest", structType)

    ))

    val row: Row = documentToRow(document, schema)
    row.schema.printTreeString()
    println(row.toSeq)

  }
}
