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
 *
 */

package com.mongodb.spark;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCursor;

import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;

import org.bson.BsonMinKey;
import org.bson.BsonMaxKey;
import org.bson.BsonValue;
import org.bson.Document;

import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassTag$;

import static scala.collection.JavaConverters.asScalaIteratorConverter;

/**
 * An RDD that executes queries on a Mongo connection and reads results.
 *
 * @param <TDocument> document type parameter
 */
public class MongoRDD<TDocument> extends RDD {
    private int    numPartitions;
    private String partitionKey;
    private String uri;
    private String database;
    private String collection;

    /**
     * Constructs a new instance.
     *
     * @param sc the spark context the RDD belongs to
     * @param partitions the number of RDD partitions
     * @param key the key to split the collection on
     * @param mongoUri the mongo client connection string uri
     * @param databaseName the database that contains the mongo collection
     * @param collectionName the mongo collection name
     */
    public MongoRDD(final SparkContext sc, final int partitions, final String key,
                    final String mongoUri, final String databaseName, final String collectionName) {
        super(sc, new ArrayBuffer<>(), ClassTag$.MODULE$.apply(Document.class));
        this.numPartitions = partitions;
        this.partitionKey = key;
        this.uri = mongoUri;
        this.database = databaseName;
        this.collection = collectionName;
    }

    /**
     * Constructs a new instance.
     *
     * @param sc the spark context the RDD belongs to
     * @param key the key to split the collection on
     * @param mongoUri the mongo client connection string uri
     * @param databaseName the database that contains the mongo collection
     * @param collectionName the mongo collection name
     */
    public MongoRDD(final SparkContext sc, final String key,
                    final String mongoUri, final String databaseName, final String collectionName) {
        this(sc, 1, key, mongoUri, databaseName, collectionName);
    }

    @Override
    public Iterator compute(final Partition split, final TaskContext context) {
        BsonValue partitionMinKey = ((MongoPartition) split).getLower();
        BsonValue partitionMaxKey = ((MongoPartition) split).getUpper();

        Document partitionQuery = new Document(this.partitionKey, new Document("$gte", partitionMinKey).append("$lt", partitionMaxKey));

        MongoCursor<Document> cursor = new MongoClient(new MongoClientURI(this.uri)).getDatabase(this.database)
                                           .getCollection(this.collection).find(partitionQuery).iterator();

        return asScalaIteratorConverter(cursor).asScala();
    }

    // TODO: currently, only supports single partitions
    @Override
    public Partition[] getPartitions() {
        return new MongoPartition[] {new MongoPartition(0, new BsonMinKey(), new BsonMaxKey())};
    }
}
