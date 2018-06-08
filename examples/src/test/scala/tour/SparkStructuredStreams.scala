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

import com.mongodb.client.MongoCollection
import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.{MongoConnector}

import scala.collection.JavaConverters._
import scala.collection.mutable
import org.apache.spark.sql._
import org.bson.Document

/**
 * The spark streams code example adapted from: http://spark.apache.org/docs/latest/streaming-programming-guide.html
 */
object SparkStructuredStreams extends TourHelper {

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
    val spark: SparkSession = getSparkSession(args) // Don't copy and paste as its already configured in the shell
    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val words: Dataset[String] = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count().map((r: Row) => WordCount(r.getAs[String](0), r.getAs[Long](1)))
    val query = wordCounts.writeStream
      .outputMode("complete")
      .foreach(new ForeachWriter[WordCount] {

        val writeConfig: WriteConfig = WriteConfig(Map("uri" -> "mongodb://localhost/test.coll"))
        var mongoConnector: MongoConnector = _
        var wordCounts: mutable.ArrayBuffer[WordCount] = _

        override def process(value: WordCount): Unit = {
          wordCounts.append(value)
        }

        override def close(errorOrNull: Throwable): Unit = {
          if (wordCounts.nonEmpty) {
            mongoConnector.withCollectionDo(writeConfig, {collection: MongoCollection[Document] =>
              collection.insertMany(wordCounts.map(wc => { new Document(wc.word, wc.count)}).asJava)
            })
          }
        }

        override def open(partitionId: Long, version: Long): Boolean = {
          mongoConnector = MongoConnector(writeConfig.asOptions)
          wordCounts = new mutable.ArrayBuffer[WordCount]()
          true
        }
      })
      .start()

    query.awaitTermination()
  }

  case class WordCount(word: String, count: Long)

}
