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
     * Parallelizes a mongo collection specified by the collection factory with the default
     * parallelism of the spark context.
     *
     * @param clazz the class of the elements in the rdd
     * @param factory a mongo collection factory
     * @param <T> the type of the objects in the RDD
     * @return the RDD
     */
    public <T> JavaRDD<T> parallelize(final Class<T> clazz, final MongoCollectionFactory<T> factory) {
        return new JavaRDD<>(new MongoRDD<>(this.sc, factory, clazz), ClassTag$.MODULE$.apply(clazz));
    }

    /**
     * Parallelizes a mongo collection specified by the collection factory with the specified parallelism.
     *
     * @param clazz the class of the elements in the rdd
     * @param factory a mongo collection factory
     * @param partitions the number of RDD partitions
     * @param <T> the type of the objects in the RDD
     * @return the RDD
     */
    public <T> JavaRDD<T> parallelize(final Class<T> clazz, final MongoCollectionFactory<T> factory, final int partitions) {
        return new JavaRDD<>(new MongoRDD<>(this.sc, factory, clazz, partitions), ClassTag$.MODULE$.apply(clazz));
    }

    /**
     * Parallelizes a mongo collection specified by the collection factory with the specified parallelism
     * and aggregation pipeline to prefilter results.
     *
     * @param clazz the class of the elements in the rdd
     * @param factory a mongo collection factory
     * @param partitions the number of RDD partitions
     * @param pipeline the aggregation pipeline
     * @param <T> the type of the objects in the RDD
     * @return the RDD
     */
    public <T> JavaRDD<T> parallelize(final Class<T> clazz, final MongoCollectionFactory<T> factory, final int partitions,
                                      final List<Bson> pipeline) {
        return new JavaRDD<>(new MongoRDD<>(this.sc, factory, clazz, partitions, pipeline), ClassTag$.MODULE$.apply(clazz));
    }

    /**
     * Parallelizes a mongo collection specified by the collection factory with the specified parallelism
     * and a query to prefilter results.
     *
     * @param clazz the class of the elements in the rdd
     * @param factory a mongo collection factory
     * @param partitions the number of RDD partitions
     * @param query the database query
     * @param <T> the type of the objects in the RDD
     * @return the RDD
     */
    public <T> JavaRDD<T> parallelize(final Class<T> clazz, final MongoCollectionFactory<T> factory, final int partitions,
                                      final Bson query) {
        return new JavaRDD<>(new MongoRDD<>(this.sc, factory, clazz, partitions, query), ClassTag$.MODULE$.apply(clazz));
    }

    /**
     * Parallelizes a mongo collection specified by the collection factory with the default parallelism of the
     * spark context and an aggregation pipeline to prefilter results.
     *
     * @param clazz the class of the elements in the rdd
     * @param factory a mongo collection factory
     * @param pipeline the aggregation pipeline
     * @param <T> the type of the objects in the RDD
     * @return the RDD
     */
    public <T> JavaRDD<T> parallelize(final Class<T> clazz, final MongoCollectionFactory<T> factory, final List<Bson> pipeline) {
        return new JavaRDD<>(new MongoRDD<>(this.sc, factory, clazz, pipeline), ClassTag$.MODULE$.apply(clazz));
    }

    /**
     * Parallelizes a mongo collection specified by the collection factory with the default parallelism of the
     * spark context and a query to prefilter results.
     *
     * @param clazz the class of the elements in the rdd
     * @param factory a mongo collection factory
     * @param query the database query
     * @param <T> the type of the objects in the RDD
     * @return the RDD
     */
    public <T> JavaRDD<T> parallelize(final Class<T> clazz, final MongoCollectionFactory<T> factory, final Bson query) {
        return new JavaRDD<>(new MongoRDD<>(this.sc, factory, clazz, query), ClassTag$.MODULE$.apply(clazz));
    }

    /**
     * Writes a JavaRDD to the collection specified by the collection factory.
     *
     * @param rdd the RDD to write
     * @param factory a mongo collection factory
     * @param mode the write mode
     * @param <TDocument> the type of the objects in the RDD
     */
    public static <TDocument> void toMongoCollection(final JavaRDD<TDocument> rdd, final MongoCollectionFactory<TDocument> factory,
                                                     final MongoWriter.WriteMode mode) {
        switch (mode) {
            case BULK_UNORDERED:
                rdd.foreachPartition(p -> new MongoBulkWriter<>(factory).write(p));
                break;
            case BULK_ORDERED:
                rdd.foreachPartition(p -> new MongoBulkWriter<>(factory, true).write(p));
                break;
            default:
                // SIMPLE
                rdd.foreachPartition(p -> new MongoSimpleWriter<>(factory).write(p));
        }
    }

    @Override
    public SparkContext sc() {
        return this.sc;
    }
}
