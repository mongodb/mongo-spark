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

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.conversions.Bson;
import scala.reflect.ClassTag$;

import java.util.List;

/**
 * An extension of the [[org.apache.spark.api.java.JavaSparkContext]] that
 * that works with MongoDB collections.
 */
public class MongoSparkContext extends JavaSparkContext {
    private SparkContext sc;

    /**
     * Constructs a new instance.
     *
     * @param sc the spark context
     */
    public MongoSparkContext(final SparkContext sc) {
        super(sc);
        this.sc = sc;
    }

    /**
     * Constructs a new instance.
     *
     * @param conf the spark configuration
     */
    public MongoSparkContext(final SparkConf conf) {
        this(new SparkContext(conf));
    }

    /**
     * Constructs a new instance.
     *
     * @param conf the spark configuration
     * @param sparkHome the location where spark is installed on cluster nodes
     */
    public MongoSparkContext(final SparkConf conf, final String sparkHome) {
        this(new SparkContext(conf.setSparkHome(sparkHome)));
    }

    /**
     * Constructs a new instance.
     *
     * @param conf the spark configuration
     * @param sparkHome the location where spark is installed on cluster nodes
     * @param jarFile the path of a JAR dependency for all tasks to be executed on this mongo spark context in the future
     */
    public MongoSparkContext(final SparkConf conf, final String sparkHome, final String jarFile) {
        this(new SparkContext(conf.setSparkHome(sparkHome).setJars(new String[] {jarFile})));
    }

    /**
     * Constructs a new instance.
     *
     * @param conf the spark configuration
     * @param sparkHome the location where spark is installed on cluster nodes
     * @param jars the paths of JAR dependencies for all tasks to be executed on this mongo spark context in the future
     */
    public MongoSparkContext(final SparkConf conf, final String sparkHome, final String[] jars) {
        this(new SparkContext(conf.setSparkHome(sparkHome).setJars(jars)));
    }

    /**
     * Parallelizes a mongo collection specified by the collection provider. The number of partitions
     * is determined by the number of chunks in a sharded collection, or the number of chunks
     * calculated by a vectorSplit for non-sharded collections (default max chunk size 64 MB).
     *
     * Note: the collection provider will be broadcasted through the MongoSparkContext.
     *
     * @param clazz the class of the elements in the rdd
     * @param provider a mongo collection provider
     * @param splitKey the minimal prefix key of the index to be used for splitting
     * @param <T> the type of the objects in the RDD
     * @return the RDD
     */
    public <T> JavaRDD<T> parallelize(final Class<T> clazz, final MongoCollectionProvider<T> provider, final String splitKey) {
        return new JavaRDD<>(new MongoRDD<>(this.sc, provider, clazz, splitKey), ClassTag$.MODULE$.apply(clazz));
    }

    /**
     * Parallelizes a mongo collection specified by the collection provider. Set maxChunkSize
     * to determine the size of the partitions based on a vectorSplit. maxChunkSize only
     * affects creating RDDs from non-sharded collections.
     *
     * Note: the collection provider will be broadcasted through the MongoSparkContext.
     *
     * @param clazz the class of the elements in the rdd
     * @param provider a mongo collection provider
     * @param splitKey the minimal prefix key of the index to be used for splitting
     * @param maxChunkSize the max chunk size for partitions in MB
     * @param <T> the type of the objects in the RDD
     * @return the RDD
     */
    public <T> JavaRDD<T> parallelize(final Class<T> clazz, final MongoCollectionProvider<T> provider, final String splitKey,
                                      final int maxChunkSize) {
        return new JavaRDD<>(new MongoRDD<>(this.sc, provider, clazz, splitKey, maxChunkSize), ClassTag$.MODULE$.apply(clazz));
    }

    /**
     * Parallelizes a mongo collection specified by the collection provider and prefiltered
     * according to the aggregation pipeline. Set maxChunkSize to determine the size of the partitions
     * based on a vectorSplit. maxChunkSize only affects creating RDDs from non-sharded collections.
     *
     * Note: the collection provider will be broadcasted through the MongoSparkContext.
     *
     * @param clazz the class of the elements in the rdd
     * @param provider a mongo collection provider
     * @param splitKey the minimal prefix key of the index to be used for splitting
     * @param maxChunkSize the max chunk size for partitions in MB
     * @param pipeline the aggregation pipeline
     * @param <T> the type of the objects in the RDD
     * @return the RDD
     */
    public <T> JavaRDD<T> parallelize(final Class<T> clazz, final MongoCollectionProvider<T> provider, final String splitKey,
                                      final int maxChunkSize, final List<Bson> pipeline) {
        return new JavaRDD<>(new MongoRDD<>(this.sc, provider, clazz, splitKey, maxChunkSize, pipeline), ClassTag$.MODULE$.apply(clazz));
    }

    /**
     * Parallelizes a mongo collection specified by the collection provider and an aggregation pipeline
     * to prefilter results. The number of partitions is determined by the number of chunks in a
     * sharded collection, or the number of chunks calculated by a vectorSplit for non-sharded
     * collections (default max chunk size 64 MB).
     *
     * Note: the collection provider will be broadcasted through the MongoSparkContext.
     *
     * @param clazz the class of the elements in the rdd
     * @param provider a mongo collection provider
     * @param splitKey the minimal prefix key of the index to be used for splitting
     * @param pipeline the aggregation pipeline
     * @param <T> the type of the objects in the RDD
     * @return the RDD
     */
    public <T> JavaRDD<T> parallelize(final Class<T> clazz, final MongoCollectionProvider<T> provider, final String splitKey,
                                      final List<Bson> pipeline) {
        return new JavaRDD<>(new MongoRDD<>(this.sc, provider, clazz, splitKey, pipeline), ClassTag$.MODULE$.apply(clazz));
    }

    @Override
    public SparkContext sc() {
        return this.sc;
    }
}
