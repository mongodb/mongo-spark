/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.spark.examples.enron;

import com.mongodb.spark.MongoClientProvider;
import com.mongodb.spark.MongoCollectionProvider;
import com.mongodb.spark.MongoSparkClientProvider;
import com.mongodb.spark.MongoSparkCollectionProvider;
import com.mongodb.spark.MongoSparkContext;
import com.mongodb.spark.MongoWriter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public final class EnronMail {
    public static void runEnronMail() {
        SparkConf sparkConf = new SparkConf();
        MongoSparkContext msc = new MongoSparkContext(sparkConf);
        MongoClientProvider clientProvider = new MongoSparkClientProvider("mongodb://localhost:27017/?maxIdleTimeMS=250");
        MongoCollectionProvider<Document> inputCollectionProvider =
                new MongoSparkCollectionProvider<>(Document.class, clientProvider, "enron_mail", "messages");

        JavaRDD<Document> rdd =
                msc.parallelize(Document.class, inputCollectionProvider, "_id")
                   .flatMapToPair(doc -> {
                       List<Tuple2<Tuple2<String, String>, Integer>> tuples = new ArrayList<>();
                       Document headers = doc.get("headers", Document.class);

                       if (headers.getString("To") != null && headers.getString("From") != null) {
                           String from = headers.getString("From");

                           for (String recipient : headers.getString("To").split(",")) {
                               String to = recipient.trim();
                               if (to.length() > 0) {
                                   tuples.add(new Tuple2<>(new Tuple2<>(from, to), 1));
                               }
                           }
                       }
                       return tuples;
                   })
                   .reduceByKey((a, b) -> a + b)
                   .map(t -> new Document("_id", new Document("f", t._1()._1())
                                                      .append("t", t._1()._2()))
                                  .append("count", t._2()));

        MongoCollectionProvider<Document> outputCollectionProvider =
                new MongoSparkCollectionProvider<>(Document.class, clientProvider, "enron_mail", "message_pairs");

        MongoWriter.writeToMongo(rdd.rdd(), outputCollectionProvider, true, false);

        msc.stop();
    }

    public static void main(final String[] args) throws Exception {
        runEnronMail();
        System.exit(0);
    }

    private EnronMail() {
    }
}
