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
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import org.bson.Document;
import org.bson.conversions.Bson;
import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassTag$;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static scala.collection.JavaConverters.asScalaIteratorConverter;

/**
 * An RDD that executes queries on a Mongo connection and reads results.
 *
 * @param <T> the type of the objects in the RDD
 */
public class MongoRDD<T> extends RDD<T> {
    private Class<T> clazz;
    private Broadcast<MongoCollectionProvider<T>> collectionProvider;
    private int maxChunkSize;
    private List<Bson> pipeline;
    private String splitKey;

    /**
     * Constructs a new instance. The number of partitions is determined by the number of chunks in a
     * sharded collection, or the number of chunks calculated by a vectorSplit for non-sharded
     * collections (default max chunk size 64 MB).
     *
     * Note: the collection provider will be broadcasted through the input SparkContext.
     *
     * @param sc the spark context the RDD belongs to
     * @param provider the mongo collection provider for the RDD
     * @param clazz the class of the elements in the RDD
     * @param splitKey the minimal prefix key of the index to be used for splitting
     */
    public MongoRDD(final SparkContext sc, final MongoCollectionProvider<T> provider, final Class<T> clazz,
                    final String splitKey) {
        this(sc, provider, clazz, splitKey, 64, Collections.emptyList());
    }

    /**
     * Constructs a new instance. Set maxChunkSize to determine the size of the partitions
     * based on a vectorSplit. maxChunkSize only affects creating RDDs from non-sharded
     * collections.
     *
     * Note: the collection provider will be broadcasted through the input SparkContext.
     *
     * @param sc the spark context the RDD belongs to
     * @param provider the mongo collection provider for the RDD
     * @param clazz the class of the elements in the RDD
     * @param splitKey the minimal prefix key of the index to be used for splitting
     * @param maxChunkSize the max chunk size for partitions in MB
     */
    public MongoRDD(final SparkContext sc, final MongoCollectionProvider<T> provider, final Class<T> clazz,
                    final String splitKey, final int maxChunkSize) {
        this(sc, provider, clazz, splitKey, maxChunkSize, Collections.emptyList());
    }

    /**
     * Constructs a new instance. The number of partitions is determined by the number of chunks in a
     * sharded collection, or the number of chunks calculated by a vectorSplit for non-sharded
     * collections (default max chunk size 64 MB).
     *
     * Note: the collection provider will be broadcasted through the input SparkContext.
     *
     * @param sc the spark context the RDD belongs to
     * @param provider the mongo collection provider for the RDD
     * @param clazz the class of the elements in the RDD
     * @param splitKey the minimal prefix key of the index to be used for splitting
     * @param pipeline the aggregation pipeline
     */
    public MongoRDD(final SparkContext sc, final MongoCollectionProvider<T> provider, final Class<T> clazz,
                    final String splitKey, final List<Bson> pipeline) {
        this(sc, provider, clazz, splitKey, 64, pipeline);
    }

    /**
     * Constructs a new instance. Set maxChunkSize to determine the size of the partitions
     * based on a vectorSplit. maxChunkSize only affects creating RDDs from non-sharded
     * collections.
     *
     * Note: the collection provider will be broadcasted through the input SparkContext.
     *
     * @param sc the spark context the RDD belongs to
     * @param provider the mongo collection provider for the RDD
     * @param clazz the class of the elements in the RDD
     * @param splitKey the minimal prefix key of the index to be used for splitting
     * @param maxChunkSize the max chunk size for partitions in MB
     * @param pipeline the aggregation pipeline
     */
    public MongoRDD(final SparkContext sc, final MongoCollectionProvider<T> provider, final Class<T> clazz,
                    final String splitKey, final int maxChunkSize, final List<Bson> pipeline) {
        super(sc, new ArrayBuffer<>(), ClassTag$.MODULE$.apply(notNull("clazz", clazz)));
        this.clazz = clazz;
        notNull("provider", provider);
        this.collectionProvider = this.sparkContext().broadcast(provider, ClassTag$.MODULE$.apply(provider.getClass()));
        this.splitKey = notNull("splitKey", splitKey);
        isTrueArgument("maxChunkSize in range [1, 1024]", maxChunkSize >= 1 && maxChunkSize <= 1024);
        this.maxChunkSize = maxChunkSize;
        this.pipeline = pipeline == null ? Collections.emptyList() : pipeline;
    }

    @Override
    public Iterator<T> compute(final Partition split, final TaskContext context) {
        MongoCursor<T> cursor = this.getCursor((MongoPartition) split);
        context.addTaskCompletionListener(new MongoTaskCompletionListener(cursor));
        return asScalaIteratorConverter(cursor).asScala();
    }

    /**
     * Helper function to retrieve the results from the collection.
     *
     * @return the cursor
     */
    private MongoCursor<T> getCursor(final MongoPartition partition) {
        Document partitionBounds = partition.getBounds();

        List<Bson> partitionPipeline = new ArrayList<>();
        partitionPipeline.add(new Document("$match", partitionBounds));
        partitionPipeline.addAll(this.pipeline);

        return this.collectionProvider.value().getCollection().aggregate(partitionPipeline, this.clazz).iterator();
    }

    @Override
    public Partition[] getPartitions() {
        final AtomicInteger count = new AtomicInteger(0);

        return this.getSplitBounds().stream()
                                    .map((document) -> new MongoPartition(count.getAndIncrement(), document))
                                    .toArray(Partition[]::new);
    }

    /**
     * Gets the split keys for the mongo collection to optimize RDD calculation.
     *
     * @return the split keys
     */
    private List<Document> getSplitBounds() {
        Document collStatsCommand = new Document("collStats", this.collectionProvider.value()
                                                                                     .getCollection()
                                                                                     .getNamespace()
                                                                                     .getCollectionName());
        Document result = this.collectionProvider.value()
                                                 .getDatabase()
                                                 .runCommand(collStatsCommand, Document.class);

        if (result.get("ok").equals(1.0)) {
            if (Boolean.TRUE.equals(result.get("sharded"))) {
                return new ShardedMongoSplitter(this.collectionProvider.value(), this.splitKey).getSplitBounds();
            } else {
                return new StandaloneMongoSplitter(this.collectionProvider.value(), this.splitKey, this.maxChunkSize).getSplitBounds();
            }
        }
        else {
            throw new SplitException("Could not get collection statistics. Server errmsg: " + result.get("errmsg"));
        }
    }
}
