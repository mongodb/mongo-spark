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

import org.apache.spark.sql.SQLContext

import org.bson.Document


/**
 * The spark SQL code example see docs/1-sparkSQL.md
 */
object SparkSQL extends TourHelper {

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
    sc.parallelize(docs.map(Document.parse)).saveToMongoDB()

    // Create SQLContext
    val sqlContext = new SQLContext(sc)

    // Import the SQL helper
    import com.mongodb.spark.sql._

    val df = sqlContext.read.mongo()
    df.printSchema()

    // Characters yoounger than 100
    df.filter(df("age") < 100).show()

    // Explicitly declaring a schema
    sqlContext.read.mongo[SparkSQL.Character]().printSchema()

    // Spark SQL
    val characters = sqlContext.loadFromMongoDB().toDF[SparkSQL.Character]()
    characters.registerTempTable("characters")

    val centenarians = sqlContext.sql("SELECT name, age FROM characters WHERE age >= 100")
    centenarians.show()

    // Save the centenarians
    centenarians.write.option("collection", "hundredClub").mongo()
    println("Reading from the 'hundredClub' collection:")
    sqlContext.read.option("collection", "hundredClub").mongo[SparkSQL.Character]().show()
  }

  case class Character(name: String, age: Int)
}
