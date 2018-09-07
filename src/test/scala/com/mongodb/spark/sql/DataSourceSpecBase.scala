/*
 * Copyright 2018 MongoDB, Inc.
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

import com.mongodb.spark._
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.sql.types.BsonCompatibility
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Dataset, SaveMode, SparkSession}
import org.bson._
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

trait DataSourceSpecBase extends RequiresMongoDB with TableDrivenPropertyChecks {
  val defaultSource: String = "com.mongodb.spark.sql.DefaultSource"

  val characters =
    """
     | {"name": "Bilbo Baggins", "age": 50}
     | {"name": "Gandalf", "age": 1000}
     | {"name": "Thorin", "age": 195}
     | {"name": "Balin", "age": 178}
     | {"name": "Kíli", "age": 77}
     | {"name": "Dwalin", "age": 169}
     | {"name": "Óin", "age": 167}
     | {"name": "Glóin", "age": 158}
     | {"name": "Fíli", "age": 82}
     | {"name": "Bombur"}""".trim.stripMargin.split("[\\r\\n]+").toSeq.map(Document.parse)

  val charactersWithAge: Seq[Document] = characters.filter(d => d.containsKey("age"))

  val expectedSchema: StructType = {
    val _idField: StructField = createStructField("_id", BsonCompatibility.ObjectId.structType, true)
    val nameField: StructField = createStructField("name", DataTypes.StringType, true)
    val ageField: StructField = createStructField("age", DataTypes.IntegerType, true)
    createStructType(Array(_idField, ageField, nameField))
  }

  implicit class SparkSessionSpecHelper(val spark: SparkSession) {
    def parallelize[T: ClassTag](data: Seq[T]): RDD[T] = spark.sparkContext.parallelize(data)
    def save[T: ClassTag](data: Seq[T]): Unit = MongoSpark.save(parallelize(data))

    def load(): DataFrame = read().load()
    def load(readConfig: ReadConfig): DataFrame = read(readConfig).load()
    def loadDS[T <: Product: TypeTag: NotNothing](): Dataset[T] = loadDS[T](ReadConfig(spark))
    def loadDS[T <: Product: TypeTag](readConfig: ReadConfig): Dataset[T] = {
      import spark.implicits._
      val schema: StructType = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
      read(readConfig).schema(schema).load().as[T]
    }

    def read(): DataFrameReader = read(ReadConfig(spark))
    def read(readConfig: ReadConfig): DataFrameReader = spark.read.format(defaultSource).options(readConfig.asOptions)

    def readConfigs: TableFor1[ReadConfig] = Table(
      "readConfig",
      ReadConfig(spark),
      ReadConfig(spark).withOption(ReadConfig.pipelineIncludeNullFiltersProperty, "false"),
      ReadConfig(spark).withOption(ReadConfig.pipelineIncludeFiltersAndProjectionsProperty, "false")
    )

    def getConf: SparkConf = spark.sparkContext.getConf
  }

  implicit class DatasetSpecHelper[T](val ds: Dataset[T]) {
    def createWriter(): DataFrameWriter[T] = createWriter(WriteConfig(ds.sparkSession))
    def createWriter(writeConfig: WriteConfig): DataFrameWriter[T] = ds.write.format(defaultSource)
      .mode(SaveMode.Append).options(writeConfig.asOptions)
    def save(): Unit = createWriter().save()
    def save(writeConfig: WriteConfig): Unit = createWriter(writeConfig).save()
  }

}
