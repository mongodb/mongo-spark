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

package tour

import org.apache.spark.sql.{SQLContext, SparkSession}

import org.bson.Document
import org.bson.types.ObjectId
import com.mongodb.spark.config.ReadConfig


/**
 * The spark SQL code example see docs/1-sparkSQL.md
 */
object SparkSQL extends TourHelper {

  //scalastyle:off method.length
  /**
   * Run this main method to see the output of this quick example or copy the code into the spark shell
   *
   * @param args takes an optional single argument for the connection string
   * @throws Throwable if an operation fails
   */
  def main(args: Array[String]): Unit = {
    val sc = getSparkContext(args) // Don't copy and paste as its already configured in the shell

    // Load sample data
    import com.mongodb.spark._
    val docs = """
                       |{"name": "Bilbo Baggins", "age": 50}
                       |{"name": "Gandalf", "age": 1000}
                       |{"name": "Thorin", "age": 195}
                       |{"name": "Balin", "age": 178}
                       |{"name": "Kíli", "age": 77}
                       |{"name": "Dwalin", "age": 169}
                       |{"name": "Óin", "age": 167}
                       |{"name": "Glóin", "age": 158}
                       |{"name": "Fíli", "age": 82}
                       |{"name": "Bombur"}""".trim.stripMargin.split("[\\r\\n]+").toSeq
    MongoSpark.save(sc.parallelize(docs.map(Document.parse)))

    // Create SparkSession
    val sparkSession = SparkSession.builder().getOrCreate()

    // Import the SQL helper
    val df = MongoSpark.load(sparkSession)
    df.printSchema()

    // Characters younger than 100
    df.filter(df("age") < 100).show()

    // Explicitly declaring a schema
    MongoSpark.load[SparkSQL.Character](sparkSession).printSchema()

    // Spark SQL
    val characters = MongoSpark.load[SparkSQL.Character](sparkSession)
    characters.createOrReplaceTempView("characters")

    val centenarians = sparkSession.sql("SELECT name, age FROM characters WHERE age >= 100")
    centenarians.show()

    // Save the centenarians
    MongoSpark.save(centenarians.write.option("collection", "hundredClub"))
    println("Reading from the 'hundredClub' collection:")
    MongoSpark.load[SparkSQL.Character](sparkSession, ReadConfig(Map("collection" -> "hundredClub"), Some(ReadConfig(sparkSession)))).show()

    // Drop database
    MongoConnector(sc).withDatabaseDo(ReadConfig(sc), db => db.drop())

    // Using the SQL helpers and StructFields helpers
    val objectId = "123400000000000000000000"
    val newDocs = Seq(new Document("_id", new ObjectId(objectId)).append("a", 1), new Document("_id", new ObjectId()).append("a", 2))
    MongoSpark.save(sc.parallelize(newDocs))

    // Set the schema using the ObjectId StructFields helper
    import org.apache.spark.sql.types.DataTypes
    import com.mongodb.spark.sql.helpers.StructFields

    val schema = DataTypes.createStructType(Array(
      StructFields.objectId("_id", nullable = false),
      DataTypes.createStructField("a", DataTypes.IntegerType, false))
    )

    // Create a dataframe with the helper functions registered
    val df1 = MongoSpark.read(sparkSession).schema(schema).option("registerSQLHelperFunctions", "true").load()

    // Query using the ObjectId string
    df1.filter(s"_id = ObjectId('$objectId')").show()
  }
  //scalastyle:on method.length

  case class Character(name: String, age: Int)
}
