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
import org.bson.BsonMinKey;
import org.bson.BsonMaxKey;
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
    private int          partitions;
    private BsonDocument query;
    private String       uri;

    /**
     * Constructs a new instance.
     *
     * @param sc the spark context the RDD belongs to
     * @param uri the mongo client connection string uri
     * @param partitions the number of RDD partitions
     * @param query the database query
     */
    public MongoRDD(final SparkContext sc, final String uri, final int partitions, final BsonDocument query) {
        super(sc, new ArrayBuffer<>(), ClassTag$.MODULE$.apply(Document.class));
        this.uri = uri;
        this.partitions = partitions;
        this.query = query;
    }

    /**
     * Constructs a new instance.
     *
     * @param sc the spark context the RDD belongs to
     * @param uri the mongo client connection string uri
     * @param partitions the number of RDD partitions
     */
    public MongoRDD(final SparkContext sc, final String uri, final int partitions) {
        this(sc, uri, partitions, new BsonDocument());
    }

    /**
     * Constructs a new instance. Uses default level of parallelism from the spark context.
     *
     * @param sc the spark context the RDD belongs to
     * @param uri the mongo client connection string uri
     */
    public MongoRDD(final SparkContext sc, final String uri) {
        this(sc, uri, sc.defaultParallelism(), new BsonDocument());
    }

    @Override
    public Iterator compute(final Partition split, final TaskContext context) {
        MongoClientURI mongoClientURI = new MongoClientURI(this.uri);

        MongoCursor<Document> cursor = new MongoClient(mongoClientURI)
                                           .getDatabase(mongoClientURI.getDatabase())
                                           .getCollection(mongoClientURI.getCollection())
                                           .find(query)
                                           .iterator();

        return asScalaIteratorConverter(cursor).asScala();
    }

    // TODO: currently, only supports single partitions
    @Override
    public Partition[] getPartitions() {
        return new MongoPartition[] {new MongoPartition(0, new BsonMinKey(), new BsonMaxKey())};
    }
}
