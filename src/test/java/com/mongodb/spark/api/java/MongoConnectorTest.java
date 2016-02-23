/*
 * Copyright 2016 MongoDB, Inc.
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

package com.mongodb.spark.api.java;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;
import com.mongodb.spark.MongoClientFactory;
import com.mongodb.spark.MongoConnector;
import com.mongodb.spark.connection.DefaultMongoClientFactory;
import org.junit.Test;
import scala.runtime.AbstractFunction1;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public final class MongoConnectorTest extends RequiresMongoDB {

    @Test
    public void shouldCreateMongoConnector() {
        String mongoClientURI = getMongoClientURI();
        List<ServerAddress> expectedServerAddresses = new ArrayList<>();
        for (String host : new MongoClientURI(mongoClientURI).getHosts()) {
            expectedServerAddresses.add(new ServerAddress(host));
        }
        MongoConnector mongoConnector = MongoConnectors.create(mongoClientURI);

        List<ServerAddress> serverAddresses = mongoConnector.withMongoClientDo(new AbstractFunction1<MongoClient, List<ServerAddress>>() {
            @Override
            public List<ServerAddress> apply(final MongoClient v1) {
                return v1.getServerAddressList();
            }
        });

        assertEquals(serverAddresses, expectedServerAddresses);
    }

    @Test
    public void shouldCreateMongoConnectorWithCustomMongoClientFactory() {
        String mongoClientURI = getMongoClientURI();
        List<ServerAddress> expectedServerAddresses = new ArrayList<>();
        for (String host : new MongoClientURI(mongoClientURI).getHosts()) {
            expectedServerAddresses.add(new ServerAddress(host));
        }
        MongoConnector mongoConnector = MongoConnectors.create(new JavaMongoClientFactory(mongoClientURI));

        List<ServerAddress> serverAddresses = mongoConnector.withMongoClientDo(new AbstractFunction1<MongoClient, List<ServerAddress>>() {
            @Override
            public List<ServerAddress> apply(final MongoClient v1) {
                return v1.getServerAddressList();
            }
        });

        assertEquals(serverAddresses, expectedServerAddresses);
    }

    class JavaMongoClientFactory implements MongoClientFactory {
        private final DefaultMongoClientFactory proxy;

        JavaMongoClientFactory(final String connectionString) {
            this.proxy = new DefaultMongoClientFactory(connectionString);
        }

        @Override
        public MongoClient create() {
            return proxy.create();
        }

        @Override
        public MongoClientFactory withServerAddress(final ServerAddress serverAddress) {
            return proxy.withServerAddress(serverAddress);
        }
    }

}
