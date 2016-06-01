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

package com.mongodb.spark.config;

import com.mongodb.WriteConcern;
import com.mongodb.spark.JavaRequiresMongoDB;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public final class WriteConfigTest extends JavaRequiresMongoDB {

    private final int localThreshold = 15;

    @Test
    public void shouldBeCreatableFromTheSparkConf() {
        WriteConfig readConfig = WriteConfig.create(getSparkConf());
        WriteConfig expectedReadConfig = WriteConfig.create(getDatabaseName(), getCollectionName(), getMongoClientURI(), localThreshold,
                WriteConcern.ACKNOWLEDGED);

        assertEquals(readConfig, expectedReadConfig);
    }

    @Test
    public void shouldBeCreatableFromAJavaMap() {
        Map<String, String> options = new HashMap<String, String>();
        options.put(WriteConfig.databaseNameProperty(), "db");
        options.put(WriteConfig.collectionNameProperty(), "collection");
        options.put(WriteConfig.localThresholdProperty(), "5");
        options.put(WriteConfig.writeConcernWProperty(), "1");
        options.put(WriteConfig.writeConcernJournalProperty(), "true");
        options.put(WriteConfig.writeConcernWTimeoutMSProperty(), "100");

        WriteConfig writeConfig = WriteConfig.create(options);
        WriteConfig expectedWriteConfig = WriteConfig.create("db", "collection", null, 5,
                WriteConcern.W1.withJournal(true).withWTimeout(100, TimeUnit.MILLISECONDS));

        assertEquals(writeConfig, expectedWriteConfig);
    }

    @Test
    public void shouldBeCreatableFromAJavaMapAndUseDefaults() {
        Map<String, String> options = new HashMap<String, String>();
        options.put(ReadConfig.databaseNameProperty(), "db");
        options.put(ReadConfig.collectionNameProperty(), "collection");

        WriteConfig writeConfig = WriteConfig.create(options, WriteConfig.create(getSparkConf()));
        WriteConfig expectedWriteConfig = WriteConfig.create("db", "collection", getMongoClientURI(), localThreshold,
                WriteConcern.ACKNOWLEDGED);

        assertEquals(writeConfig, expectedWriteConfig);
    }
}
