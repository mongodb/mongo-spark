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
        this.sc = sc;
        this.uri = uri;
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
     * Parallelizes a mongo collection.
     *
     * @param partitions the number of RDD partitions
     * @param key the key to partition on
     * @return the RDD
     * @throws IllegalArgumentException if partitions is not nonnegative
     * @throws IllegalArgumentException if key is null
     * @throws IllegalArgumentException if mongo client uri does not contain a database name
     * @throws IllegalArgumentException if mongo client uri does not contain a collection name
     */
    public MongoJavaRDD<TDocument> parallelize(final int partitions, final String key)
            throws IllegalArgumentException {
        if (partitions < 0) {
            throw new IllegalArgumentException("partitions must be > 0");
        }
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        if (this.uri.getDatabase() == null) {
            throw new IllegalArgumentException("uri must specify a database");
        }
        if (this.uri.getCollection() == null) {
            throw new IllegalArgumentException("uri must specify a collection");
        }

        return new MongoJavaRDD<>(new MongoRDD<>(this.sc(), partitions, key, this.uri.getURI(), this.uri.getDatabase(),
                                                 this.uri.getCollection()));
    }

    /**
     * Parallelizes a mongo collection. Defaults number of partitions to 1.
     *
     * @param key the key to partition on
     * @return the RDD
     * @throws IllegalArgumentException if key is null
     * @throws IllegalArgumentException if mongo client uri does not contain a database name
     * @throws IllegalArgumentException if mongo client uri does not contain a collection name
     */
    public MongoJavaRDD<TDocument> parallelize(final String key) throws IllegalArgumentException {
        return parallelize(1, key);
    }

    @Override
    public SparkContext sc() {
        return this.sc;
    }
}
