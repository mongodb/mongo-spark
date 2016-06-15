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


import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
 * The spark streams code example adapted from: http://spark.apache.org/docs/latest/streaming-programming-guide.html
 */
object SparkStreams extends TourHelper {

  /**
   * Run this main method to see the output of this quick example or copy the code into the spark shell
   *
   * See: http://spark.apache.org/docs/latest/streaming-programming-guide.html
   * Requires: Netcat running on localhost:9999
   *
   * @param args takes an optional single argument for the connection string
   * @throws Throwable if an operation fails
   */
  def main(args: Array[String]): Unit = {
    val sc = getSparkContext(args) // Don't copy and paste as its already configured in the shell

    import com.mongodb.spark.sql._
    import org.apache.spark.streaming._

    val ssc = new StreamingContext(sc, Seconds(1))

    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.foreachRDD({ rdd =>
      val sparkSession = SparkSessionSingleton.getInstance(rdd.sparkContext)
      import sparkSession.implicits._

      val wordCounts = rdd.map({ case (word: String, count: Int) => WordCount(word, count) }).toDF()
      wordCounts.write.mode("append").mongo()
    })

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate

  }

  case class WordCount(word: String, count: Int)

  /** Lazily instantiated singleton instance of SQLContext */
  object SparkSessionSingleton {

    @transient private var instance: SparkSession = _

    def getInstance(sparkContext: SparkContext): SparkSession = {
      if (Option(instance).isEmpty) {
        instance = SparkSession.builder().getOrCreate()
      }
      instance
    }
  }

}
