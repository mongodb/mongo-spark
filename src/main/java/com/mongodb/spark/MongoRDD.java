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
import org.bson.BsonDocument;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassTag$;

import static scala.collection.JavaConverters.asScalaIteratorConverter;

/**
 * An RDD that executes queries on a Mongo connection and reads results.
 *
 * @param <T> the type of the objects in the RDD
 */
public class MongoRDD<T> extends RDD<T> {
    private Class<T>     clazz;
    private int          partitions;
    private BsonDocument query;
    private String       uri;

    /**
     * Constructs a new instance.
     *
     * @param sc the spark context the RDD belongs to
     * @param uri the mongo client connection string uri
     * @param clazz the [[java.lang.Class]] of the elements in the RDD
     * @param partitions the number of RDD partitions
     * @param query the database query
     */
    public MongoRDD(final SparkContext sc, final String uri, final Class<T> clazz, final int partitions, final BsonDocument query) {
        super(sc, new ArrayBuffer<>(), ClassTag$.MODULE$.apply(clazz));
        this.clazz = clazz;
        this.uri = uri;
        this.partitions = partitions;
        this.query = query;
    }

    /**
     * Constructs a new instance.
     *
     * @param sc the spark context the RDD belongs to
     * @param uri the mongo client connection string uri
     * @param clazz the [[java.lang.Class]] of the elements in the RDD
     * @param partitions the number of RDD partitions
     */
    public MongoRDD(final SparkContext sc, final String uri, final Class<T> clazz, final int partitions) {
        this(sc, uri, clazz, partitions, new BsonDocument());
    }

    /**
     * Constructs a new instance.
     *
     * @param sc the spark context the RDD belongs to
     * @param uri the mongo client connection string uri
     * @param clazz the [[java.lang.Class]] of the elements in the RDD
     * @param query the database query
     */
    public MongoRDD(final SparkContext sc, final String uri, final Class<T> clazz, final BsonDocument query) {
        this(sc, uri, clazz, sc.defaultParallelism(), query);
    }

    /**
     * Constructs a new instance. Uses default level of parallelism from the spark context.
     *
     * @param sc the spark context the RDD belongs to
     * @param uri the mongo client connection string uri
     * @param clazz the [[java.lang.Class]] of the elements in the RDD
     */
    public MongoRDD(final SparkContext sc, final String uri, final Class<T> clazz) {
        this(sc, uri, clazz, sc.defaultParallelism(), new BsonDocument());
    }

    // TODO: currently, only supports single partitions
    @Override
    public Iterator<T> compute(final Partition split, final TaskContext context) {
        MongoClientURI mongoClientURI = new MongoClientURI(this.uri);

        MongoCursor<T> cursor = new MongoClient(mongoClientURI)
                                    .getDatabase(mongoClientURI.getDatabase())
                                    .getCollection(mongoClientURI.getCollection())
                                    .find(this.query, this.clazz)
                                    .iterator();

        return asScalaIteratorConverter(cursor).asScala();
    }

    // TODO: currently, only supports single partitions
    @Override
    public Partition[] getPartitions() {
        return new MongoPartition[] {new MongoPartition(0, new BsonMinKey(), new BsonMaxKey())};
    }
}
