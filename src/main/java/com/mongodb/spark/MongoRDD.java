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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.bson.Document;
import org.bson.conversions.Bson;
import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassTag$;
import scala.runtime.BoxedUnit;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static scala.collection.JavaConverters.asJavaIteratorConverter;
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

    /**
     * Writes a JavaRDD to the collection specified by the collection factory. If inserting,
     * it is recommended to remove the _id's from the Documents being inserted.
     *
     * @param rdd the RDD to write
     * @param factory a mongo collection factory
     * @param mode the write mode
     * @param <TDocument> the type of the objects in the RDD
     */
    public static <TDocument> void toMongoCollection(final JavaRDD<TDocument> rdd, final MongoCollectionFactory<TDocument> factory,
                                                     final MongoWriter.WriteMode mode) {
        toMongoCollection(rdd.rdd(), factory, mode);
    }

    /**
     * Writes a RDD to the collection specified by the collection factory. If inserting,
     * it is recommended to remove the _id's from the Documents being inserted.
     *
     * @param rdd the RDD to write
     * @param factory a mongo collection factory
     * @param mode the write mode
     * @param <TDocument> the type of the objects in the RDD
     */
    public static <TDocument> void toMongoCollection(final RDD<TDocument> rdd, final MongoCollectionFactory<TDocument> factory,
                                                     final MongoWriter.WriteMode mode) {
        switch (mode) {
            case BULK_UNORDERED_REPLACE:
            case BULK_UNORDERED_UPDATE:
                rdd.foreachPartition(new SerializableAbstractFunction1<Iterator<TDocument>, BoxedUnit>() {
                    @Override
                    public BoxedUnit apply(final Iterator<TDocument> p) {
                        new MongoBulkWriter<>(factory, mode, false).write(asJavaIteratorConverter(p).asJava());
                        return BoxedUnit.UNIT;
                    }
                });
                break;
            case BULK_ORDERED_REPLACE:
            case BULK_ORDERED_UPDATE:
                rdd.foreachPartition(new SerializableAbstractFunction1<Iterator<TDocument>, BoxedUnit>() {
                    @Override
                    public BoxedUnit apply(final Iterator<TDocument> p) {
                        new MongoBulkWriter<>(factory, mode, true).write(asJavaIteratorConverter(p).asJava());
                        return BoxedUnit.UNIT;
                    }
                });
                break;
            default:
                // SIMPLE
                rdd.foreachPartition(new SerializableAbstractFunction1<Iterator<TDocument>, BoxedUnit>() {
                    @Override
                    public BoxedUnit apply(final Iterator<TDocument> p) {
                        new MongoSimpleWriter<>(factory).write(asJavaIteratorConverter(p).asJava());
                        return BoxedUnit.UNIT;
                    }
                });
        }
    }

    @Override
    public Iterator<T> compute(final Partition split, final TaskContext context) {
        return asScalaIteratorConverter(this.getCursor((MongoPartition) split)).asScala();
    }

    /**
     * Helper function to retrieve the results from the collection.
     *
     * @return the results of the
     */
    // TODO: add support for query filters, limits, modifiers, projections, skips, sorts
    private MongoCursor<T> getCursor(final MongoPartition partition) {
        Bson partitionBounds = partition.getBounds();

        if (this.query == null && this.pipeline == null) {
            return this.collectionFactory.getCollection().find(partitionBounds, this.clazz).iterator();
        }
        if (this.query != null) {
            return this.collectionFactory.getCollection().find(partitionBounds, this.clazz).filter(query).iterator();
        }

        List<Bson> partitionPipeline = new ArrayList<>(1 + this.pipeline.size());
        partitionPipeline.add(new Document("$match", partitionBounds));
        partitionPipeline.addAll(this.pipeline);

        return this.collectionFactory.getCollection().aggregate(partitionPipeline, this.clazz).iterator();
    }

    // TODO: currently, this.partitions actually means maxChunkSize
    @Override
    public Partition[] getPartitions() {
        List<Bson> splitBounds = this.getSplitBounds(this.partitions);
        int numPartitions = splitBounds.size();
        MongoPartition[] mongoPartitions = new MongoPartition[numPartitions];

        for (int i = 0; i < numPartitions; i++) {
            mongoPartitions[i] = new MongoPartition(i, splitBounds.get(i));
        }

        return mongoPartitions;
    }

    /**
     * Gets the split keys for the mongo collection to optimize RDD calculation.
     *
     * @param maxChunkSize the max chunk size desired for each partition
     * @return the split keys
     */
    private List<Bson> getSplitBounds(final int maxChunkSize) {
        List<Bson> splitBounds;

        // TODO: get stats on the mongo - is it standalone? shard?
        splitBounds = new MongoStandaloneSplitter(this.collectionFactory, null).getSplitBounds(maxChunkSize);

        return splitBounds;
    }
}
