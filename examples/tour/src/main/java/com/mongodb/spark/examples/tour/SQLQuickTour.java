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

package com.mongodb.spark.examples.tour;

import com.mongodb.spark.MongoClientProvider;
import com.mongodb.spark.MongoCollectionProvider;
import com.mongodb.spark.MongoSparkClientProvider;
import com.mongodb.spark.MongoSparkCollectionProvider;
import com.mongodb.spark.MongoSparkContext;
import com.mongodb.spark.sql.DocumentRowConverter;
import com.mongodb.spark.sql.SchemaProvider;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;

public final class SQLQuickTour {
    public static void main(final String[] args) {
        // Setup parameters for the rest of the program.
        String uri = "mongodb://localhost:27017";
        String database = "spark_test";
        String collection = "sql_quick_tour";
        String splitKey = "_id";
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("SQL Quick Tour");

        // Setup a client provider.
        MongoClientProvider clientProvider = new MongoSparkClientProvider(uri);

        // Setup a collection provider using the client provider - thus, can
        // reuse the instantiated client amongst multiple partitions that
        // need connections to the collection.
        MongoCollectionProvider<Document> collectionProvider =
                new MongoSparkCollectionProvider<>(Document.class, clientProvider, database, collection);

        // Setup the test db.
        List<Document> documents = Arrays.asList(new Document("a", 1), new Document("a", 2), new Document("a", 3));
        collectionProvider.getCollection().drop();
        collectionProvider.getCollection().insertMany(documents);

        // Setup the MongoSparkContext.
        MongoSparkContext msc = new MongoSparkContext(sparkConf);

        // Setup the SQLContext.
        SQLContext sqlContext = new SQLContext(msc);

        // Infer the structure of the documents in the collection.
        StructType schema = SchemaProvider.getSchema(collectionProvider, 3);

        // Get the RDD and map each document to a Spark SQL row using the
        // inferred schema.
        JavaRDD<Row> rdd = msc.parallelize(Document.class, collectionProvider, splitKey)
                              .map(doc -> DocumentRowConverter.documentToRow(doc, schema));

        // Create a DataFrame from the RDD of rows, and register the table with
        // the SQLContext.
        DataFrame frame = sqlContext.createDataFrame(rdd, schema);
        frame.registerTempTable("test");

        // Execute a SQL query on the table and store the results.
        List<Row> results = sqlContext.sql("SELECT * FROM test").collectAsList();
    }

    private SQLQuickTour() {
    }
}
