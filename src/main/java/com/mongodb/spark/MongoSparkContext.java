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
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import scala.reflect.ClassTag$;

import java.util.List;

/**
 * An extension of the [[org.apache.spark.api.java.JavaSparkContext]] that
 * that works with MongoDB collections.
 */
public class MongoSparkContext extends JavaSparkContext {
    private SparkContext       sc;
    private MongoClientFactory clientFactory;

    /**
     * Constructs a new instance.
     *
     * @param sc the spark context
     */
    public MongoSparkContext(final SparkContext sc) {
        super(sc);
        this.sc = sc;
        this.clientFactory = new MongoSparkClientFactory(sc.getConf());
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
     * Parallelizes a mongo collection. Querying may be performed by passing
     * a BsonDocument to query the database with before parallelizing results.
     *
     * @param <T> the type of the objects in the RDD
     * @param clazz the [[java.lang.Class]] of the elements in the RDD
     * @param database the database name
     * @param collection the collection name
     * @param partitions the number of RDD partitions
     * @param query the database query
     * @return the RDD
     * @throws IllegalArgumentException if clazz is null
     * @throws IllegalArgumentException if partitions is not nonnegative
     */
    public <T> JavaRDD<T> parallelize(final Class<T> clazz, final String database, final String collection, final int partitions,
                                      final Bson query) throws IllegalArgumentException {
        return this.parallelize(clazz, new MongoSparkCollectionFactory<>(clazz, this.clientFactory, database, collection), partitions, null,
                                query);
    }

    /**
     * Parallelizes a mongo collection.
     *
     * @param <T> the type of the objects in the RDD
     * @param clazz the [[java.lang.Class]] of the elements in the RDD
     * @param database the database name
     * @param collection the collection name
     * @param partitions the number of RDD partitions
     * @return the RDD
     * @throws IllegalArgumentException if clazz is null
     * @throws IllegalArgumentException if partitions is not nonnegative
     */
    public <T> JavaRDD<T> parallelize(final Class<T> clazz, final String database, final String collection, final int partitions)
            throws IllegalArgumentException {
        return this.parallelize(clazz, new MongoSparkCollectionFactory<>(clazz, this.clientFactory, database, collection), partitions, null,
                                new BsonDocument());
    }

    /**
     * Parallelizes a mongo collection. Uses default level of parallelism from the spark context.
     *
     * @param <T> the type of the objects in the RDD
     * @param clazz the [[java.lang.Class]] of the elements in the RDD
     * @param database the database name
     * @param collection the collection name
     * @param query the database query
     * @return the RDD
     * @throws IllegalArgumentException if clazz is null
     * @throws IllegalArgumentException if partitions is not nonnegative
     */
    public <T> JavaRDD<T> parallelize(final Class<T> clazz, final String database, final String collection, final Bson query)
            throws IllegalArgumentException {
        return this.parallelize(clazz, new MongoSparkCollectionFactory<>(clazz, this.clientFactory, database, collection),
                                this.sc.defaultParallelism(), null, query);
    }

    /**
     * Parallelizes a mongo collection.
     *
     * @param <T> the type of the objects in the RDD
     * @param clazz the [[java.lang.Class]] of the elements in the RDD
     * @param database the database name
     * @param collection the collection name
     * @return the RDD
     * @throws IllegalArgumentException if clazz is null
     * @throws IllegalArgumentException if partitions is not nonnegative
     */
    public <T> JavaRDD<T> parallelize(final Class<T> clazz, final String database, final String collection)
            throws IllegalArgumentException {
        return this.parallelize(clazz, new MongoSparkCollectionFactory<>(clazz, this.clientFactory, database, collection),
                                this.sc.defaultParallelism(), null, new BsonDocument());
    }

    /**
     * Parallelizes a mongo collection. Aggregation may be performed by passing
     * a pipeline to query the database before parallelizing the results.
     *
     * @param <T> the type of the objects in the RDD
     * @param clazz the [[java.lang.Class]] of the elements in the RDD
     * @param database the database name
     * @param collection the collection name
     * @param partitions the number of RDD partitions
     * @param pipeline the aggregation pipeline
     * @return the RDD
     * @throws IllegalArgumentException if clazz is null
     * @throws IllegalArgumentException if partitions is not nonnegative
     */
    public <T> JavaRDD<T> parallelize(final Class<T> clazz, final String database, final String collection, final int partitions,
                                      final List<Bson> pipeline) throws IllegalArgumentException {
        return this.parallelize(clazz, new MongoSparkCollectionFactory<>(clazz, this.clientFactory, database, collection), partitions,
                                pipeline, null);
    }

    /**
     * Parallelizes a mongo collection.
     *
     * @param <T> the type of the objects in the RDD
     * @param clazz the [[java.lang.Class]] of the elements in the RDD
     * @param database the database name
     * @param collection the collection name
     * @param pipeline the aggregation pipeline
     * @return the RDD
     * @throws IllegalArgumentException if clazz is null
     * @throws IllegalArgumentException if partitions is not nonnegative
     */
    public <T> JavaRDD<T> parallelize(final Class<T> clazz, final String database, final String collection, final List<Bson> pipeline)
            throws IllegalArgumentException {
        return this.parallelize(clazz, new MongoSparkCollectionFactory<>(clazz, this.clientFactory, database, collection),
                                this.sc.defaultParallelism(), pipeline, null);
    }

    /**
     * Parallelizes a mongo collection.
     *
     * @param clazz the [[java.lang.Class]] of the elements in the RDD
     * @param factory a mongo collection factory
     * @param partitions the number of RDD partitions
     * @param pipeline the aggregation pipeline
     * @param query the database query
     * @param <T> the type of the objects in the RDD
     * @return the RDD
     * @throws IllegalArgumentException if clazz is null
     * @throws IllegalArgumentException if partitions is not nonnegative
     */
    public <T> JavaRDD<T> parallelize(final Class<T> clazz, final MongoCollectionFactory<T> factory, final int partitions,
                                      final List<Bson> pipeline, final Bson query) throws IllegalArgumentException {
        return new JavaRDD<>(new MongoRDD<>(this.sc, factory, clazz, partitions, pipeline, query), ClassTag$.MODULE$.apply(clazz));
    }

    @Override
    public SparkContext sc() {
        return this.sc;
    }
}
