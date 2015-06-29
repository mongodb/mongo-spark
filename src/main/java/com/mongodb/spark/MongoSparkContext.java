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
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

import org.bson.Document;

/**
 * An extension of the [[org.apache.spark.api.java.JavaSparkContext]] that
 * that works with MongoDB collections.
 */
public class MongoSparkContext extends JavaSparkContext {
    private final SparkContext          sc;
    private       MongoClient           client;
    private       List<MongoCredential> clientCredentials;
    private       List<String>          clientHosts;
    private       MongoClientOptions    clientOptions;

    /**
     * Gets the list of mongo client credentials.
     *
     * @return the mongo client credentials
     */
    private List<MongoCredential> getClientCredentials() {
        if (this.clientCredentials == null) {
            this.clientCredentials = new ArrayList<>();
        }
        return clientCredentials;
    }

    /**
     * Gets the list of mongo client hosts.
     *
     * @return the mongo client hosts
     */
    private List<String> getClientHosts() {
        if (this.clientHosts == null) {
            this.clientHosts = new ArrayList<>();
        }
        return clientHosts;
    }

    /**
     * Gets the mongo client options.
     *
     * @return the mongo client options
     */
    private MongoClientOptions getClientOptions() {
        if (this.clientOptions == null) {
            this.clientOptions = new MongoClientOptions.Builder().build();
        }
        return clientOptions;
    }

    /**
     * Gets the mongo client.
     *
     * @return the mongo client
     */
    public MongoClient getClient() {
        if (this.client == null) {
            List<MongoCredential> credentials = getClientCredentials();
            List<String> hosts = getClientHosts();
            MongoClientOptions options = getClientOptions();

            List<ServerAddress> serverAddresses = new ArrayList<>();
            if (hosts.isEmpty()) {
                serverAddresses.add(new ServerAddress());
            }
            else {
                hosts.forEach(host -> serverAddresses.add(new ServerAddress(host)));
            }
            this.client = new MongoClient(serverAddresses, credentials, options);
        }
        return client;
    }

    /**
     * Constructs a new instance.
     *
     * @param conf the spark configuration
     */
    public MongoSparkContext(final SparkConf conf) {
        this(new SparkContext(conf), null, null, null);
    }

    /**
     * Constructs a new instance.
     *
     * @param sc the spark context
     */
    public MongoSparkContext(final SparkContext sc) {
        this(sc, null, null, null);
    }

    /**
     * Constructs a new instance.
     *
     * @param sc the spark context
     * @param clientCredentials the mongo client credentials
     */
    public MongoSparkContext(final SparkContext sc, final List<MongoCredential> clientCredentials) {
        this(sc, clientCredentials, null, null);
    }

    /**
     * Constructs a new instance.
     *
     * @param sc the spark context
     * @param clientCredentials the mongo client credentials
     * @param clientHosts the mongo client hosts
     */
    public MongoSparkContext(final SparkContext sc, final List<MongoCredential> clientCredentials, final List<String> clientHosts) {
        this(sc, clientCredentials, clientHosts, null);
    }

    /**
     * Constructs a new instance.
     *
     * @param sc the spark context
     * @param clientCredentials the mongo client credentials
     * @param clientHosts the mongo client hosts
     * @param clientOptions the mongo client options
     */
    public MongoSparkContext(final SparkContext sc, final List<MongoCredential> clientCredentials, final List<String> clientHosts,
                             final MongoClientOptions clientOptions) {
        super(sc);
        this.sc = sc;
        this.clientCredentials = clientCredentials;
        this.clientHosts = clientHosts;
        this.clientOptions = clientOptions;
    }

    /**
     * Constructs a new instance.
     *
     * @param conf the spark configuration
     * @param clientCredentials the mongo client credentials
     * @param clientHosts the mongo client hosts
     * @param clientOptions the mongo client options
     */
    public MongoSparkContext(final SparkConf conf, final List<MongoCredential> clientCredentials, final List<String> clientHosts,
                             final MongoClientOptions clientOptions) {
        this(new SparkContext(conf), clientCredentials, clientHosts, clientOptions);
    }

    /**
     * Constructs a new instance.
     *
     * @param master the spark cluster manager to connect to
     * @param appName the application name
     * @param clientCredentials the mongo client credentials
     * @param clientHosts the mongo client hosts
     * @param clientOptions the mongo client options
     */
    public MongoSparkContext(final String master, final String appName, final List<MongoCredential> clientCredentials,
                             final List<String> clientHosts, final MongoClientOptions clientOptions) {
        this(new SparkContext(master, appName), clientCredentials, clientHosts, clientOptions);
    }

    /**
     * Constructs a new instance.
     *
     * @param master the spark cluster manager to connect to
     * @param appName the application name
     * @param conf the spark configuration
     * @param clientCredentials the mongo client credentials
     * @param clientHosts the mongo client hosts
     * @param clientOptions the mongo client options
     */
    public MongoSparkContext(final String master, final String appName, final SparkConf conf, final List<MongoCredential> clientCredentials,
                             final List<String> clientHosts, final MongoClientOptions clientOptions) {
        this(new SparkContext(master, appName, conf), clientCredentials, clientHosts, clientOptions);
    }

    /**
     * Constructs a new instance.
     *
     * @param master the spark cluster manager to connect to
     * @param appName the application name
     * @param sparkHome the path where spark is installed on cluster nodes
     * @param jarFile the path of a JAR dependency for all tasks to be executed on this mongo spark context in the future
     * @param clientCredentials the mongo client credentials
     * @param clientHosts the mongo client hosts
     * @param clientOptions the mongo client options
     */
    public MongoSparkContext(final String master, final String appName, final String sparkHome, final String jarFile,
                             final List<MongoCredential> clientCredentials, final List<String> clientHosts,
                             final MongoClientOptions clientOptions) {
        this(new SparkContext(
                        new SparkConf().setMaster(master).setAppName(appName).setSparkHome(sparkHome).setJars(new String[] {jarFile})),
             clientCredentials, clientHosts, clientOptions);
    }

    /**
     * Constructs a new instance.
     *
     * @param master the spark cluster manager to connect to
     * @param appName the application name
     * @param sparkHome the location where spark is installed on cluster nodes
     * @param jars the paths of JAR dependencies for all tasks to be executed on this mongo spark context in the future
     * @param clientCredentials the mongo client credentials
     * @param clientHosts the mongo client hosts
     * @param clientOptions the mongo client options
     */
    public MongoSparkContext(final String master, final String appName, final String sparkHome, final String[] jars,
                             final List<MongoCredential> clientCredentials, final List<String> clientHosts,
                             final MongoClientOptions clientOptions) {
        this(new SparkContext(new SparkConf().setMaster(master).setAppName(appName).setSparkHome(sparkHome).setJars(jars)),
                clientCredentials, clientHosts, clientOptions);
    }

    /**
     * Constructs a new instance.
     *
     * @param sc the spark context
     * @param uri the mongo client connection string uri
     */
    public MongoSparkContext(final SparkContext sc, final MongoClientURI uri) {
        this(sc, Collections.singletonList(uri.getCredentials()), uri.getHosts(), uri.getOptions());
    }

    /**
     * Constructs a new instance.
     *
     * @param conf the spark configuration
     * @param uri the mongo client connection string uri
     */
    public MongoSparkContext(final SparkConf conf, final MongoClientURI uri) {
        this(new SparkContext(conf), Collections.singletonList(uri.getCredentials()), uri.getHosts(), uri.getOptions());
    }

    /**
     * Parallelizes a mongo collection.
     *
     * @param databaseName the name of the database for the collection
     * @param collectionName the name of the collection to parallelize
     * @param partitions the number of partitions
     * @return the RDD
     */
    public JavaRDD<Document> parallelize(final String databaseName, final String collectionName, final int partitions) {
        if (collectionName == null) {
            throw new IllegalArgumentException("collectionName must not be null");
        }
        if (partitions < 0) {
            throw new IllegalArgumentException("partitions must be > 0");
        }

        MongoClient mongoClient = getClient();
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(collectionName);

        List<Document> documents = new ArrayList<>();
        collection.find().into(documents);

        return super.parallelize(documents, partitions);
    }

    /**
     * Parallelizes a mongo collection.
     *
     * @param databaseName the name of the database for the collection
     * @param collectionName the collection to parallelize
     * @return the RDD
     */
    public JavaRDD<Document> parallelize(final String databaseName, final String collectionName) {
        return parallelize(databaseName, collectionName, 1);
    }

    @Override
    public SparkContext sc() {
        return sc;
    }
}
