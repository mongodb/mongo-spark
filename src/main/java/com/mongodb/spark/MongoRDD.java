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
import org.bson.Document;
import org.bson.conversions.Bson;
import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassTag$;

import java.util.ArrayList;
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
    private Class<T> clazz;
    private MongoCollectionFactory<T> collectionFactory;
    private int partitions;
    private List<Bson> pipeline;
    private String splitKey;

    /**
     * Constructs a new instance.
     *
     * @param sc the spark context the RDD belongs to
     * @param factory the mongo collection factory for the RDD
     * @param clazz the class of the elements in the RDD
     * @param splitKey the minimal prefix key of the index to be used for splitting
     */
    public MongoRDD(final SparkContext sc, final MongoCollectionFactory<T> factory, final Class<T> clazz, final String splitKey) {
        this(sc, factory, clazz, splitKey, sc.defaultParallelism(), null);
    }

    /**
     * Constructs a new instance.
     *
     * @param sc the spark context the RDD belongs to
     * @param factory the mongo collection factory for the RDD
     * @param clazz the class of the elements in the RDD
     * @param splitKey the minimal prefix key of the index to be used for splitting
     * @param partitions the number of RDD partitions
     */
    public MongoRDD(final SparkContext sc, final MongoCollectionFactory<T> factory, final Class<T> clazz, final String splitKey,
                    final int partitions) {
        this(sc, factory, clazz, splitKey, partitions, null);
    }

    /**
     * Constructs a new instance.
     *
     * @param sc the spark context the RDD belongs to
     * @param factory the mongo collection factory for the RDD
     * @param clazz the class of the elements in the RDD
     * @param splitKey the minimal prefix key of the index to be used for splitting
     * @param pipeline the aggregation pipeline
     */
    public MongoRDD(final SparkContext sc, final MongoCollectionFactory<T> factory, final Class<T> clazz, final String splitKey,
                    final List<Bson> pipeline) {
        this(sc, factory, clazz, splitKey, sc.defaultParallelism(), pipeline);
    }

    /**
     * Constructs a new instance.
     *
     * @param sc the spark context the RDD belongs to
     * @param factory the mongo collection factory for the RDD
     * @param clazz the class of the elements in the RDD
     * @param splitKey the minimal prefix key of the index to be used for splitting
     * @param partitions the number of RDD partitions
     * @param pipeline the aggregation pipeline
     */
    public MongoRDD(final SparkContext sc, final MongoCollectionFactory<T> factory, final Class<T> clazz, final String splitKey,
                    final int partitions, final List<Bson> pipeline) {
        super(sc, new ArrayBuffer<>(), ClassTag$.MODULE$.apply(notNull("clazz", clazz)));
        this.clazz = clazz;
        this.collectionFactory = notNull("factory", factory);
        this.splitKey = notNull("splitKey", splitKey);
        isTrueArgument("partitions > 0", partitions > 0);
        this.partitions = partitions;
        this.pipeline = pipeline;
    }

    @Override
    public Iterator<T> compute(final Partition split, final TaskContext context) {
        return asScalaIteratorConverter(this.getCursor((MongoPartition) split)).asScala();
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
        if (this.pipeline != null) {
            partitionPipeline.addAll(this.pipeline);
        }

        return this.collectionFactory.getCollection().aggregate(partitionPipeline, this.clazz).iterator();
    }

    @Override
    public Partition[] getPartitions() {
        List<Document> splitBounds = this.getSplitBounds();
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
     * @return the split keys
     */
    private List<Document> getSplitBounds() {
        Document collStatsCommand = new Document("collStats", this.collectionFactory.getCollection().getNamespace().getCollectionName());
        Document result = this.collectionFactory.getDatabase().runCommand(collStatsCommand, Document.class);

        if (result.get("ok").equals(1.0)) {
            if (Boolean.TRUE.equals(result.get("sharded"))) {
                return new ShardedMongoSplitter(this.collectionFactory, this.splitKey).getSplitBounds();
            }
            else {
                // TODO: currently, this.partitions actually means maxChunkSize
                return new StandaloneMongoSplitter(this.collectionFactory, this.splitKey, this.partitions).getSplitBounds();
            }
        }
        else {
            throw new SplitException("Could not get collection statistics. Server errmsg: " + result.get("errmsg"));
        }
    }
}
