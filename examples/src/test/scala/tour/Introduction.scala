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

/**
 * The Introduction code example see docs/0-introduction.md
 */
object Introduction extends TourHelper {

  /**
   * Run this main method to see the output of this quick example or copy the code into the spark shell
   *
   * @param args takes an optional single argument for the connection string
   * @throws Throwable if an operation fails
   */
  def main(args: Array[String]): Unit = {
    val sc = getSparkContext(args) // Don't copy and paste as its already configured in the shell

    import com.mongodb.spark._

    // Saving data from an RDD to MongoDB
    import org.bson.Document
    val documents = sc.parallelize((1 to 10).map(i => Document.parse(s"{test: $i}")))
    MongoSpark.save(documents)

    // Saving data with a custom WriteConfig
    import com.mongodb.spark.config._
    val writeConfig = WriteConfig(Map("collection" -> "spark", "writeConcern.w" -> "majority"), Some(WriteConfig(sc)))

    val sparkDocuments = sc.parallelize((1 to 10).map(i => Document.parse(s"{spark: $i}")))
    MongoSpark.save(sparkDocuments, writeConfig)

    // Loading and analyzing data from MongoDB
    val rdd = MongoSpark.load(sc)
    println(rdd.count)
    println(rdd.first.toJson)

    // Loading data with a custom ReadConfig
    val readConfig = ReadConfig(Map("collection" -> "spark", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
    val customRdd = MongoSpark.load(sc, readConfig)
    println(customRdd.count)
    println(customRdd.first.toJson)

    // Filtering an rdd in Spark
    val filteredRdd = rdd.filter(doc => doc.getInteger("test") > 5)
    println(filteredRdd.count)
    println(filteredRdd.first.toJson)

    // Filtering an rdd using an aggregation pipeline before passing to Spark
    val aggregatedRdd = rdd.withPipeline(Seq(Document.parse("{ $match: { test : { $gt : 5 } } }")))
    println(aggregatedRdd.count)
    println(aggregatedRdd.first.toJson)
  }

}
