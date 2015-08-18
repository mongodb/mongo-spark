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
import com.mongodb.MongoClientURI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Implementation of a client provider. Used to share a client amongst partitions in a worker node.
 */
public class MongoSparkClientProvider implements MongoClientProvider {
    private static final Log LOG = LogFactory.getLog(MongoSparkClientProvider.class);

    private transient MongoClient client;
    private MongoClientOptionsBuilderInitializer initializer;
    private String uri;

    /**
     * Constructs an instance.
     *
     * @param uri the mongo client connection uri
     */
    public MongoSparkClientProvider(final String uri) {
        this.uri = notNull("uri", uri);
    }

    /**
     * Constructs an instance.
     *
     * @param uri the mongo client connection uri
     * @param initializer the mongo client options builder initializer
     */
    public MongoSparkClientProvider(final String uri, final MongoClientOptionsBuilderInitializer initializer) {
        this.uri = notNull("uri", uri);
        this.initializer = notNull("initializer", initializer);
    }

    @Override
    public MongoClient getClient() {
        if (this.client == null) {
            LOG.debug("Instantiating a MongoClient");

            if (this.initializer != null) {
                this.client = new MongoClient(new MongoClientURI(this.uri, this.initializer.initialize()));
            }
            else {
                this.client = new MongoClient(new MongoClientURI(this.uri));
            }
        }

        return this.client;
    }

    @Override
    public void close() throws IOException {
        if (this.client != null) {
            LOG.debug("Closing a MongoClient");

            this.client.close();
            this.client = null;
        }
    }
}
