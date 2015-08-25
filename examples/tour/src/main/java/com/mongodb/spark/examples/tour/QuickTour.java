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

import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.spark.MongoClientOptionsBuilderInitializer;
import com.mongodb.spark.MongoClientProvider;
import com.mongodb.spark.MongoCollectionProvider;
import com.mongodb.spark.MongoRDD;
import com.mongodb.spark.MongoSparkClientOptionsBuilderInitializer;
import com.mongodb.spark.MongoSparkClientProvider;
import com.mongodb.spark.MongoSparkCollectionProvider;
import com.mongodb.spark.MongoSparkContext;
import com.mongodb.spark.MongoWriter;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Arrays;
import java.util.List;

public final class QuickTour {
    public static void main(final String[] args) {
        // Setup parameters for the rest of the program.
        String uri = "mongodb://localhost:27017";
        String database = "spark_test";
        String collection = "quick_tour";
        String splitKey = "_id";
        Boolean isUpsert = Boolean.TRUE;
        Boolean isOrdered = Boolean.TRUE;

        // Setup a builder initializer - the function will be executed on each
        // worker node any time a client is instantiated.
        // Uses a 'custom' codec registry; in any case, an instantiated codec
        // registry is not serializable, so this shows how to work around the
        // serializability issue for providing options beyond those supported
        // in the URI connection string.
        MongoClientOptionsBuilderInitializer builderInitializer =
                new MongoSparkClientOptionsBuilderInitializer(() -> {
                    CodecRegistry codecRegistry = CodecRegistries.fromCodecs(new DocumentCodec());
                    return new Builder().codecRegistry(codecRegistry);
                });

        // Setup a client provider.
        MongoClientProvider clientProvider =
                new MongoSparkClientProvider(uri, builderInitializer);

        // Setup a collection provider using the client provider - thus, can
        // reuse the instantiated client amongst multiple partitions that
        // need connections to the collection.
        MongoCollectionProvider<Document> collectionProvider =
                new MongoSparkCollectionProvider<>(Document.class, clientProvider, database, collection);

        // Setup the test db.
        List<Document> documents = Arrays.asList(new Document("a", 1), new Document("a", 2), new Document("a", 3));
        collectionProvider.getCollection().drop();
        collectionProvider.getCollection().insertMany(documents);

        // Setup the Spark Context
        SparkContext sc = new SparkContext("local", "Quick Tour");

        // Setup the MongoSparkContext - this provides handy `parallelize`
        // methods for creating RDDs.
        MongoSparkContext msc = new MongoSparkContext(sc);

        // Examples of ways to create JavaRDDs and MongoRDDs.
        JavaRDD<Document> jrdd = msc.parallelize(Document.class, collectionProvider, splitKey);

        // MongoRDD extends RDD and overrides the compute and getPartitions method.
        // However, any transformations and actions will return a new RDD.
        MongoRDD<Document> rdd = new MongoRDD<>(sc, collectionProvider, Document.class, splitKey);

        // Write an RDD to the collection specified by the collectionProvider.
        MongoWriter.writeToMongo(rdd, collectionProvider, isUpsert, isOrdered);
    }

    private QuickTour() {
    }
}
