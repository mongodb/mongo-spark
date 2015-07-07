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

import com.mongodb.MongoClientURI;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.BsonDocument;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An extension of the [[org.apache.spark.api.java.JavaSparkContext]] that
 * that works with MongoDB collections.
 *
 * @param <TDocument> document type parameter
 */
public class MongoSparkContext<TDocument> extends JavaSparkContext {
    private SparkContext   sc;
    private MongoClientURI uri;

    /**
     * Constructs a new instance.
     *
     * @param sc the spark context
     * @param uri the mongo client uri
     */
    public MongoSparkContext(final SparkContext sc, final MongoClientURI uri) {
        super(sc);
        this.sc = notNull("sc", sc);
        this.uri = notNull("uri", uri);
        notNull("uri database", this.uri.getDatabase());
        notNull("uri collection", this.uri.getCollection());
    }

    /**
     * Constructs a new instance.
     *
     * @param conf the spark configuration
     * @param uri the mongo client uri
     */
    public MongoSparkContext(final SparkConf conf, final MongoClientURI uri) {
        this(new SparkContext(conf), uri);
    }

    /**
     * Constructs a new instance.
     *
     * @param master the spark cluster manager to connect to
     * @param appName the application name
     * @param uri the mongo client uri
     */
    public MongoSparkContext(final String master, final String appName, final MongoClientURI uri) {
        this(new SparkContext(master, appName), uri);
    }

    /**
     * Constructs a new instance.
     *
     * @param master the spark cluster manager to connect to
     * @param appName the application name
     * @param conf the spark configuration
     * @param uri the mongo client uri
     */
    public MongoSparkContext(final String master, final String appName, final SparkConf conf, final MongoClientURI uri) {
        this(new SparkContext(master, appName, conf), uri);
    }

    /**
     * Constructs a new instance.
     *
     * @param master the spark cluster manager to connect to
     * @param appName the application name
     * @param sparkHome the path where spark is installed on cluster nodes
     * @param jarFile the path of a JAR dependency for all tasks to be executed on this mongo spark context in the future
     * @param uri the mongo client uri
     */
    public MongoSparkContext(final String master, final String appName, final String sparkHome, final String jarFile,
                             final MongoClientURI uri) {
        this(new SparkContext(
                     new SparkConf().setMaster(master).setAppName(appName).setSparkHome(sparkHome).setJars(new String[] {jarFile})), uri);
    }

    /**
     * Constructs a new instance.
     *
     * @param master the spark cluster manager to connect to
     * @param appName the application name
     * @param sparkHome the location where spark is installed on cluster nodes
     * @param jars the paths of JAR dependencies for all tasks to be executed on this mongo spark context in the future
     * @param uri the mongo client uri
     */
    public MongoSparkContext(final String master, final String appName, final String sparkHome, final String[] jars,
                             final MongoClientURI uri) {
        this(new SparkContext(new SparkConf().setMaster(master).setAppName(appName).setSparkHome(sparkHome).setJars(jars)), uri);
    }

    /**
     * Parallelizes a mongo collection. Querying may be performed by passing
     * a BsonDocument to query the database with before parallelizing results.
     *
     * @param partitions the number of RDD partitions
     * @param query the database query
     * @return the RDD
     * @throws IllegalArgumentException if partitions is not nonnegative
     */
    public MongoJavaRDD<TDocument> parallelize(final int partitions, final BsonDocument query) throws IllegalArgumentException {
        if (partitions < 0) {
            throw new IllegalArgumentException("partitions must be > 0");
        }
        if (query == null) {
            throw new IllegalArgumentException("query must not be null");
        }

        return new MongoJavaRDD<>(new MongoRDD<>(this.sc, this.uri.getURI(), partitions, query));
    }

    /**
     * Parallelizes a mongo collection.
     *
     * @param partitions the number of RDD partitions
     * @return the RDD
     * @throws IllegalArgumentException if partitions is not nonnegative
     */
    public MongoJavaRDD<TDocument> parallelize(final int partitions) throws IllegalArgumentException {
        return parallelize(partitions, new BsonDocument());
    }

    /**
     * Parallelizes a mongo collection. Uses default level of parallelism from the spark context.
     *
     * @param query the database query
     * @return the RDD
     * @throws IllegalArgumentException if partitions is not nonnegative
     */
    public MongoJavaRDD<TDocument> parallelize(final BsonDocument query) throws IllegalArgumentException {
        return parallelize(this.sc.defaultParallelism(), query);
    }

    /**
     * Parallelizes a mongo collection.
     *
     * @return the RDD
     * @throws IllegalArgumentException if partitions is not nonnegative
     */
    public MongoJavaRDD<TDocument> parallelize() throws IllegalArgumentException {
        return parallelize(this.sc.defaultParallelism(), new BsonDocument());
    }

    @Override
    public SparkContext sc() {
        return this.sc;
    }
}
