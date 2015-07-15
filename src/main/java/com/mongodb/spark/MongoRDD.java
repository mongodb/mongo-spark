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

import com.mongodb.client.MongoCursor;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.conversions.Bson;
import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassTag$;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static scala.collection.JavaConverters.asScalaIteratorConverter;

/**
 * An RDD that executes queries on a Mongo connection and reads results.
 *
 * @param <T> the type of the objects in the RDD
 */
public class MongoRDD<T> extends RDD<T> {
    private Class<T>                  clazz;
    private int                       partitions;
    private List<Bson>                pipeline;
    private Bson                      query;
    private MongoCollectionFactory<T> collectionFactory;

    /**
     * Constructs a new instance.
     *
     * @param sc the spark context the RDD belongs to
     * @param factory the mongo collection factory for the RDD
     * @param clazz the class of the elements in the RDD
     */
    public MongoRDD(final SparkContext sc, final MongoCollectionFactory<T> factory, final Class<T> clazz) {
        this(sc, factory, clazz, sc.defaultParallelism(), null, null);
    }

    /**
     *
     *
     * @param sc the spark context the RDD belongs to
     * @param factory the mongo collection factory for the RDD
     * @param clazz the class of the elements in the RDD
     * @param partitions the number of RDD partitions
     */
    public MongoRDD(final SparkContext sc, final MongoCollectionFactory<T> factory, final Class<T> clazz, final int partitions) {
        this(sc, factory, clazz, partitions, null, null);
    }

    /**
     * Constructs a new instance.
     *
     * @param sc the spark context the RDD belongs to
     * @param factory the mongo collection factory for the RDD
     * @param clazz the class of the elements in the RDD
     * @param pipeline the aggregation pipeline
     */
    public MongoRDD(final SparkContext sc, final MongoCollectionFactory<T> factory, final Class<T> clazz,
                    final List<Bson> pipeline) {
        this(sc, factory, clazz, sc.defaultParallelism(), pipeline, null);
    }

    /**
     *
     *
     * @param sc the spark context the RDD belongs to
     * @param factory the mongo collection factory for the RDD
     * @param clazz the class of the elements in the RDD
     * @param partitions the number of RDD partitions
     * @param pipeline the aggregation pipeline
     */
    public MongoRDD(final SparkContext sc, final MongoCollectionFactory<T> factory, final Class<T> clazz, final int partitions,
                    final List<Bson> pipeline) {
        this(sc, factory, clazz, partitions, pipeline, null);
    }

    /**
     * Constructs a new instance.
     *
     * @param sc the spark context the RDD belongs to
     * @param factory the mongo collection factory for the RDD
     * @param clazz the class of the elements in the RDD
     * @param query the database query
     */
    public MongoRDD(final SparkContext sc, final MongoCollectionFactory<T> factory, final Class<T> clazz,
                    final Bson query) {
        this(sc, factory, clazz, sc.defaultParallelism(), null, query);
    }

    /**
     * Constructs a new instance
     *
     * @param sc the spark context the RDD belongs to
     * @param factory the mongo collection factory for the RDD
     * @param clazz the class of the elements in the RDD
     * @param partitions the number of RDD partitions
     * @param query the database query
     */
    public MongoRDD(final SparkContext sc, final MongoCollectionFactory<T> factory, final Class<T> clazz, final int partitions,
                    final Bson query) {
        this(sc, factory, clazz, partitions, null, query);
    }

    /**
     * Private helper to construct a new instance. Since it is private, we can ensure that upon calling the
     * constructor either pipeline, query, or both will be null.
     *
     * @param sc the spark context the RDD belongs to
     * @param factory the mongo collection factory for the RDD
     * @param clazz the class of the elements in the RDD
     * @param partitions the number of RDD partitions
     * @param pipeline the aggregation pipeline
     * @param query the database query
     */
    private MongoRDD(final SparkContext sc, final MongoCollectionFactory<T> factory, final Class<T> clazz, final int partitions,
                     final List<Bson> pipeline, final Bson query) {
        super(sc, new ArrayBuffer<>(), ClassTag$.MODULE$.apply(notNull("clazz", clazz)));
        this.clazz = clazz;
        this.collectionFactory = notNull("factory", factory);
        isTrueArgument("partitions > 0", partitions > 0);
        this.partitions = partitions;
        this.pipeline = pipeline;
        this.query = query;
    }

    @Override
    public Iterator<T> compute(final Partition split, final TaskContext context) {
        return asScalaIteratorConverter(this.getCursor(split)).asScala();
    }

    /**
     * Helper function to retrieve the results from the collection.
     *
     * @return the results of the
     */
    // TODO: add support for partition min and max keys
    private MongoCursor<T> getCursor(final Partition partition) {
        if (this.query == null && this.pipeline == null) {
            return this.collectionFactory.getCollection().find(this.clazz).iterator();
        }
        if (this.query != null) {
            return this.collectionFactory.getCollection().find(this.query, this.clazz).iterator();
        }

        return this.collectionFactory.getCollection().aggregate(this.pipeline, this.clazz).iterator();
    }

    // TODO: currently, only supports DUPLICATED partitions
    @Override
    public Partition[] getPartitions() {
        MongoPartition[] mongoPartitions = new MongoPartition[this.partitions];

        for (int i = 0; i < this.partitions; i++) {
            mongoPartitions[i] = new MongoPartition(i, new BsonMinKey(), new BsonMaxKey());
        }

        return mongoPartitions;
    }
}
