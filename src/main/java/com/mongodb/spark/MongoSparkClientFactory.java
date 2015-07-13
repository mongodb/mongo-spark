/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.spark;

import com.mongodb.DBDecoderFactory;
import com.mongodb.DBEncoderFactory;
import com.mongodb.DefaultDBDecoder;
import com.mongodb.DefaultDBEncoder;
import com.mongodb.LazyDBDecoder;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.util.JSON;
import org.apache.spark.SparkConf;
import org.bson.codecs.configuration.CodecRegistry;
import scala.Tuple2;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Implementation of a client factory. Used to share a client amongst partitions in a worker node.
 */
class MongoSparkClientFactory implements MongoClientFactory {
    private MongoClient                client;
    private List<ServerAddress>        hosts;
    private List<MongoCredential>      credentials;
    private MongoClientOptions.Builder builder;
    private Tuple2<String, String>[]   conf;

    /**
     * Constructs a new instance.
     *
     * @param conf the spark configuration
     */
    public MongoSparkClientFactory(final SparkConf conf) {
        notNull("conf", conf);
        this.conf = conf.getAll();
    }

    /**
     * Gets a mongo client. If a client has already been constructed, the existing client is returned.
     * This behavior allows for reuse of the client on individual worker nodes.
     *
     * @return a mongo client
     */
    public MongoClient getClient() {
        if (this.client == null) {
            if (this.hosts == null) {
                this.parseConf();
            }

            this.client = new MongoClient(this.hosts, this.credentials, this.builder.build());
        }

        return this.client;
    }

    /**
     * Parses a [[org.apache.spark.SparkConf]] for spark.mongo.* settings, and stores the settings
     * for configuring new mongo client instances.
     */
    private void parseConf() {
        this.hosts = new ArrayList<>();
        this.credentials = new ArrayList<>();
        this.builder = new Builder();

        Map<String, String> properties = new HashMap<>();

        for (Tuple2<String, String> property : conf) {
            properties.put(property._1(), property._2());
        }

        String authMechanism = null;
        String authUsername = null;
        String authSource = null;
        String authPassword = null;
        Map<String, Object> authMechanismProperties = null;

        if (properties.containsKey("spark.mongo.hosts")) {
            this.hosts.add(new ServerAddress(properties.get("spark.mongo.hosts")));
        }
        if (properties.containsKey("spark.mongo.auth.mechanism")) {
            authMechanism = properties.get("spark.mongo.auth.mechanism");
        }
        if (properties.containsKey("spark.mongo.auth.userName")) {
            authUsername = properties.get("spark.mongo.auth.userName");
        }
        if (properties.containsKey("spark.mongo.auth.source")) {
            authSource = properties.get("spark.mongo.auth.source");
        }
        if (properties.containsKey("spark.mongo.auth.password")) {
            authPassword = properties.get("spark.mongo.auth.password");
        }
        if (properties.containsKey("spark.mongo.auth.mechanismProperties")) {
            // TODO: proper type-checking; JSON.parse will either return a key-value map, a boxed primitive, or null
            authMechanismProperties = (Map<String, Object>) JSON.parse(properties.get("spark.mongo.auth.mechanismProperties"));
        }
        if (properties.containsKey("spark.mongo.options.description")) {
            this.builder.description(properties.get("spark.mongo.options.description"));
        }
        if (properties.containsKey("spark.mongo.options.readPreference")) {
            this.builder.readPreference(ReadPreference.valueOf(properties.get("spark.mongo.options.readPreference")));
        }
        if (properties.containsKey("spark.mongo.options.writeConcern")) {
            this.builder.writeConcern(WriteConcern.valueOf(properties.get("spark.mongo.options.writeConcern")));
        }
        if (properties.containsKey("spark.mongo.options.codecRegistry")) {
            setBuilderCodecRegistry(properties.get("spark.mongo.options.codecRegistry"));
        }
        if (properties.containsKey("spark.mongo.options.minConnectionsPerHost")) {
            this.builder.minConnectionsPerHost(Integer.valueOf(properties.get("spark.mongo.options.minConnectionsPerHost")));
        }
        if (properties.containsKey("spark.mongo.options.maxConnectionsPerHost")) {
            this.builder.connectionsPerHost(Integer.valueOf(properties.get("spark.mongo.options.maxConnectionsPerHost")));
        }
        if (properties.containsKey("spark.mongo.options.threadsAllowedToBlockForConnectionMultiplier")) {
            this.builder.threadsAllowedToBlockForConnectionMultiplier(
                Integer.valueOf(properties.get("spark.mongo.options.threadsAllowedToBlockForConnectionMultiplier")));
        }
        if (properties.containsKey("spark.mongo.options.serverSelectionTimeout")) {
            this.builder.serverSelectionTimeout(Integer.valueOf(properties.get("spark.mongo.options.serverSelectionTimeout")));
        }
        if (properties.containsKey("spark.mongo.options.maxWaitTime")) {
            this.builder.maxWaitTime(Integer.valueOf(properties.get("spark.mongo.options.maxWaitTime")));
        }
        if (properties.containsKey("spark.mongo.options.maxConnectionIdleTime")) {
            this.builder.maxConnectionIdleTime(Integer.valueOf(properties.get("spark.mongo.options.maxConnectionIdleTime")));
        }
        if (properties.containsKey("spark.mongo.options.maxConnectionLifeTime")) {
            this.builder.maxConnectionLifeTime(Integer.valueOf(properties.get("spark.mongo.options.maxConnectionLifeTime")));
        }
        if (properties.containsKey("spark.mongo.options.connectTimeout")) {
            this.builder.connectTimeout(Integer.valueOf(properties.get("spark.mongo.options.connectTimeout")));
        }
        if (properties.containsKey("spark.mongo.options.socketTimeout")) {
            this.builder.socketTimeout(Integer.valueOf(properties.get("spark.mongo.options.socketTimeout")));
        }
        if (properties.containsKey("spark.mongo.options.socketKeepAlive")) {
            this.builder.socketKeepAlive(Boolean.valueOf(properties.get("spark.mongo.options.socketKeepAlive")));
        }
        if (properties.containsKey("spark.mongo.options.sslEnabled")) {
            this.builder.sslEnabled(Boolean.valueOf(properties.get("spark.mongo.options.sslEnabled")));
        }
        if (properties.containsKey("spark.mongo.options.sslInvalidHostNameAllowed")) {
            this.builder.sslInvalidHostNameAllowed(Boolean.valueOf(properties.get("spark.mongo.options.sslInvalidHostNameAllowed")));
        }
        if (properties.containsKey("spark.mongo.options.alwaysUseMBeans")) {
            this.builder.alwaysUseMBeans(Boolean.valueOf(properties.get("spark.mongo.options.alwaysUseMBeans")));
        }
        if (properties.containsKey("spark.mongo.options.heartbeatFrequency")) {
            this.builder.heartbeatFrequency(Integer.valueOf(properties.get("spark.mongo.options.heartbeatFrequency")));
        }
        if (properties.containsKey("spark.mongo.options.minHeartbeatFrequency")) {
            this.builder.minHeartbeatFrequency(Integer.valueOf(properties.get("spark.mongo.options.minHeartbeatFrequency")));
        }
        if (properties.containsKey("spark.mongo.options.heartbeatConnectTimeout")) {
            this.builder.heartbeatConnectTimeout(Integer.valueOf(properties.get("spark.mongo.options.heartbeatConnectTimeout")));
        }
        if (properties.containsKey("spark.mongo.options.heartbeatSocketTimeout")) {
            this.builder.heartbeatSocketTimeout(Integer.valueOf(properties.get("spark.mongo.options.heartbeatSocketTimeout")));
        }
        if (properties.containsKey("spark.mongo.options.localThreshold")) {
            this.builder.localThreshold(Integer.valueOf(properties.get("spark.mongo.options.localThreshold")));
        }
        if (properties.containsKey("spark.mongo.options.requiredReplicaSetName")) {
            this.builder.requiredReplicaSetName(properties.get("spark.mongo.options.requiredReplicaSetName"));
        }
        if (properties.containsKey("spark.mongo.options.dbDecoderFactory")) {
            this.setBuilderDBDecoderFactory(properties.get("spark.mongo.option.dbDecoderFactory"));
        }
        if (properties.containsKey("spark.mongo.options.dbEncoderFactory")) {
            this.setBuilderDBEncoderFactory(properties.get("spark.mongo.option.dbEncoderFactory"));
        }
        if (properties.containsKey("spark.mongo.options.socketFactory")) {
            this.setBuilderSocketFactory(properties.get("spark.mongo.option.socketFactory"));
        }
        if (properties.containsKey("spark.mongo.options.cursorFinalizerEnabled")) {
            this.builder.cursorFinalizerEnabled(Boolean.valueOf(properties.get("spark.mongo.options.cursorFinalizerEnabled")));
        }

        if (authUsername != null) {
            this.addCredential(authMechanism, authUsername, authSource, authPassword, authMechanismProperties);
        }
    }

    /**
     * Helper to set the builders codec registry from `spark.mongo.options.codecRegistry` in the [[org.apache.spark.SparkConf]].
     *
     * @param codecRegistryName the name of the codec registry class
     */
    private void setBuilderCodecRegistry(final String codecRegistryName) {
        try {
            this.builder.codecRegistry((CodecRegistry) Class.forName(codecRegistryName).newInstance());
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            // use default
            // TODO: notify user
        }
    }

    /**
     * Helper to set the builders db decoder factory from `spark.mongo.options.dbDecoderFactory` in the [[org.apache.spark.SparkConf]].
     *
     * @param dbDecoderFactoryName the name of the db decoder factory class
     */
    private void setBuilderDBDecoderFactory(final String dbDecoderFactoryName) {
        switch (dbDecoderFactoryName) {
            case "DefaultDBDecoderFactory":
                this.builder.dbDecoderFactory(DefaultDBDecoder.FACTORY);
                break;
            case "LazyDBDecoderFactory":
                this.builder.dbDecoderFactory(LazyDBDecoder.FACTORY);
                break;
            default:
                try {
                    this.builder.dbDecoderFactory((DBDecoderFactory) Class.forName(dbDecoderFactoryName).newInstance());
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                    // use default
                    // TODO: notify user
                    this.builder.dbDecoderFactory(DefaultDBDecoder.FACTORY);
                }
        }
    }

    /**
     * Helper to set the builders db encoder factory from `spark.mongo.options.dbEncoderFactory` in the [[org.apache.spark.SparkConf]].
     *
     * @param dbEncoderFactoryName the name of the db encoder factory class
     */
    private void setBuilderDBEncoderFactory(final String dbEncoderFactoryName) {
        if (dbEncoderFactoryName.equals("DefaultDBEncoderFactory")) {
            this.builder.dbEncoderFactory(DefaultDBEncoder.FACTORY);
        }
        else {
            try {
                this.builder.dbEncoderFactory((DBEncoderFactory) Class.forName(dbEncoderFactoryName).newInstance());
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                // use default
                // TODO: notify user
                this.builder.dbEncoderFactory(DefaultDBEncoder.FACTORY);
            }
        }
    }

    /**
     * Helper to set the builders socket factory from `spark.mongo.options.socketFactory` in the [[org.apache.spark.SparkConf]].
     *
     * @param socketFactoryName the name of the socket factory class
     */
    private void setBuilderSocketFactory(final String socketFactoryName) {
        switch (socketFactoryName) {
            case "SocketFactory":
                this.builder.socketFactory(SocketFactory.getDefault());
                break;
            case "SSLSocketFactory":
                this.builder.socketFactory(SSLSocketFactory.getDefault());
                break;
            default:
                try {
                    this.builder.socketFactory((SocketFactory) Class.forName(socketFactoryName).newInstance());
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                    // use default
                    // TODO: notify user
                    this.builder.socketFactory(SocketFactory.getDefault());
                }
        }
    }

    /**
     * Helper to add the appropriate credential to the list of mongo credentials.
     *
     * @param mechanism the credential mechanism
     * @param username the username
     * @param source the source where the user is defined, typically the database
     * @param password the password
     * @param mechanismProperties additional mechanism properties
     * @throws IllegalArgumentException if the authorization mechanism is unsupported
     */
    private void addCredential(final String mechanism, final String username, final String source, final String password,
                               final Map<String, Object> mechanismProperties) throws IllegalArgumentException {
        MongoCredential credential;

        if (mechanism == null) {
            credential = MongoCredential.createCredential(notNull("spark.mongo.auth.username", username),
                                                          notNull("spark.mongo.auth.source", source),
                                                          notNull("spark.mongo.auth.password", password.toCharArray()));
        }
        else {
            switch (mechanism) {
                case "GSSAPI":
                    credential = MongoCredential.createGSSAPICredential(notNull("spark.mongo.auth.username", username));
                    break;
                case "PLAIN":
                    credential = MongoCredential.createPlainCredential(notNull("spark.mongo.auth.username", username),
                                                                       notNull("spark.mongo.auth.source", source),
                                                                       notNull("spark.mongo.auth.password", password.toCharArray()));
                    break;
                case "MONGODB-X509":
                    credential = MongoCredential.createMongoX509Credential(notNull("spark.mongo.auth.username", username));
                    break;
                case "MONGODB-CR":
                    credential = MongoCredential.createMongoCRCredential(notNull("spark.mongo.auth.username", username),
                                                                         notNull("spark.mongo.auth.source", source),
                                                                         notNull("spark.mongo.auth.password", password.toCharArray()));
                    break;
                case "SCRAM-SHA-1":
                    credential = MongoCredential.createScramSha1Credential(notNull("spark.mongo.auth.username", username),
                                                                           notNull("spark.mongo.auth.source", source),
                                                                           notNull("spark.mongo.auth.password", password.toCharArray()));
                    break;
                default:
                    throw new IllegalArgumentException("unsupported mongo credential authorization mechanism");
            }
        }

        if (mechanismProperties != null) {
            for (String key : mechanismProperties.keySet()) {
                credential = credential.withMechanismProperty(key, mechanismProperties.get(key));
            }
        }

        this.credentials.add(credential);
    }

    @Override
    public void cleanUp() {
        if (this.client != null) {
            this.client.close();
            this.client = null;
        }
    }
}
