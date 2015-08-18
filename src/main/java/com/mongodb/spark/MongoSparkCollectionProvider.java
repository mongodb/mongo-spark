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

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Implementation of a collection provider.
 *
 * @param <T> type of objects in the collection
 */
public class MongoSparkCollectionProvider<T> implements MongoCollectionProvider<T> {
    private static final Log LOG = LogFactory.getLog(MongoSparkCollectionProvider.class);

    private Class<T> clazz;
    private MongoClientProvider clientProvider;
    private String collection;
    private String database;
    private transient MongoCollection<T> mongoCollection;

    /**
     * Constructs a new instance.
     *
     * @param clazz the java.lang.Class of the elements in the RDD
     * @param clientProvider the client provider
     * @param database the database name on the client
     * @param collection the collection name in the database
     */
    public MongoSparkCollectionProvider(final Class<T> clazz, final MongoClientProvider clientProvider, final String database,
                                        final String collection) {
        this.clazz = notNull("clazz", clazz);
        this.clientProvider = notNull("clientProvider", clientProvider);
        this.database = notNull("database", database);
        this.collection = notNull("collection", collection);
    }

    @Override
    public MongoCollection<T> getCollection() {
        if (this.mongoCollection == null) {
            LOG.debug("Instantiating a MongoCollection for " + this.database + "." + this.collection);

            this.mongoCollection = this.clientProvider.getClient()
                                                      .getDatabase(this.database)
                                                      .getCollection(this.collection, this.clazz);
        }

        return this.mongoCollection;
    }

    @Override
    public MongoDatabase getDatabase() {
        return this.clientProvider.getClient()
                                  .getDatabase(this.database);
    }

    @Override
    public MongoClient getClient() {
        return this.clientProvider.getClient();
    }
}
