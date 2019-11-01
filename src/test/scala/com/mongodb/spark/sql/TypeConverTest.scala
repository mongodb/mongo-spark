package com.mongodb.spark.sql

import com.mongodb.spark.sql.MapFunctions.documentToRow
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, NullType, StringType, StructField, TimestampType, _}
import org.bson.{BsonDocument, BsonValue}

object TypeConverTest {

  def main(args: Array[String]): Unit = {
    println("hello")
    //    val newDocs = Seq(new Document("_id", new ObjectId(objectId)).append("a", 1), new Document("_id", new ObjectId()).append("a", 2))
    val document: BsonDocument = BsonDocument.
      parse(
        """{name: 'John', age: "", height: "192",sfb: 'true', money:'25.5',arrcol:[{k1:'a',k2:10}],
          |mtest:{k1:'a',k2:'b'},arr2:["00050001", "00040008"]}""".stripMargin)
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

    val arrayStruct2 = ArrayType(StructType(Seq(
      StructField("k1", StringType, true),
      StructField("k2", DoubleType, true))
    ), true)

    val schema = StructType(Seq(
      StructField("name", StringType, nullable = false),
      StructField("age", DoubleType, nullable = false),
      StructField("height", IntegerType, nullable = true),
      StructField("sfb", BooleanType, nullable = true),
      StructField("money", DoubleType, nullable = true),
      StructField("arrcol", arrayStruct),
      StructField("mtest", structType),
      StructField("arr2", arrayStruct2)

    ))
    println("hiveSchema.fieldNames=>" )
    schema.printTreeString()
    println("hiveSchema.fieldNames=>", schema.fieldNames.mkString(","))

    val row: Row = documentToRow(document, schema)
    row.schema.printTreeString()
    println(row.toSeq)

    val spark = SparkSession.builder().master("local[*]").
      appName(this.getClass.getSimpleName)
      .getOrCreate()
    println(spark)

  }
}
