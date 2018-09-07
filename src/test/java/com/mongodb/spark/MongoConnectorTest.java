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

package com.mongodb.spark;

import com.mongodb.MongoClient;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.connection.DefaultMongoClientFactory;
import org.apache.spark.SparkConf;
import org.junit.Test;
import scala.runtime.AbstractFunction1;

import static org.junit.Assert.assertTrue;

public final class MongoConnectorTest extends JavaRequiresMongoDB {

    @Test
    public void shouldCreateMongoConnector() {
        MongoConnector mongoConnector = MongoConnector.create(getSparkConf());
        Boolean created = mongoConnector.withMongoClientDo(new AbstractFunction1<MongoClient, Boolean>() {
            @Override
            public Boolean apply(final MongoClient v1) {
                return true;
            }
        });

        assertTrue(created);
    }

    @Test
    public void shouldUseTheMongoClientCache() {
        Boolean sameClient = MongoConnector.create(getSparkConf()).withMongoClientDo(new AbstractFunction1<MongoClient, Boolean>() {
            @Override
            public Boolean apply(final MongoClient client1) {
                return MongoConnector.create(getSparkConf()).withMongoClientDo(new AbstractFunction1<MongoClient, Boolean>() {
                    @Override
                    public Boolean apply(final MongoClient client2) {
                        return client1.equals(client2);
                    }
                });
            }
        });
        assertTrue(sameClient);
    }

    @Test
    public void shouldCreateMongoConnectorFromJavaSparkContext() {
        MongoConnector mongoConnector = MongoConnector.create(getJavaSparkContext());
        Boolean created = mongoConnector.withMongoClientDo(new AbstractFunction1<MongoClient, Boolean>() {
            @Override
            public Boolean apply(final MongoClient v1) {
                return true;
            }
        });

        assertTrue(created);
    }

    @Test
    public void shouldCreateMongoConnectorWithCustomMongoClientFactory() {
        MongoConnector mongoConnector = MongoConnector.create(new JavaMongoClientFactory(getSparkConf()));
        Boolean created = mongoConnector.withMongoClientDo(new AbstractFunction1<MongoClient, Boolean>() {
            @Override
            public Boolean apply(final MongoClient v1) {
                return true;
            }
        });

        assertTrue(created);
    }

    private class JavaMongoClientFactory implements MongoClientFactory {
        private final DefaultMongoClientFactory proxy;

        JavaMongoClientFactory(final SparkConf sparkConf) {
            this.proxy = DefaultMongoClientFactory.apply(ReadConfig.create(sparkConf).asOptions());
        }

        @Override
        public MongoClient create() {
            return proxy.create();
        }

        @Override
        public boolean equals(final Object o) {
            return proxy.equals(o);
        }

        @Override
        public int hashCode() {
            return proxy.hashCode();
        }
    }
}
