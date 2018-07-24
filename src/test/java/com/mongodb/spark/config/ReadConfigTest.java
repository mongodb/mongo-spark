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

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.client.model.Collation;
import com.mongodb.spark.JavaRequiresMongoDB;
import org.bson.BsonDocument;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public final class ReadConfigTest extends JavaRequiresMongoDB {

    @Test
    public void shouldBeCreatableFromTheSparkConf() {
        ReadConfig readConfig = ReadConfig.create(getSparkConf().remove("spark.mongodb.input.partitioner"));
        ReadConfig expectedReadConfig = ReadConfig.create(getDatabaseName(), getCollectionName(), getMongoClientURI(), 1000,
                "DefaultMongoPartitioner$", new HashMap<String, String>(), 15, ReadPreference.primary(), ReadConcern.DEFAULT,
                Collation.builder().build(), new BsonDocument());

        assertEquals(readConfig.databaseName(), expectedReadConfig.databaseName());
        assertEquals(readConfig.collectionName(), expectedReadConfig.collectionName());
        assertEquals(readConfig.connectionString(), expectedReadConfig.connectionString());
        assertEquals(readConfig.sampleSize(), expectedReadConfig.sampleSize());
        assertEquals(readConfig.partitioner(), expectedReadConfig.partitioner());
        assertEquals(readConfig.partitionerOptions(), expectedReadConfig.partitionerOptions());
        assertEquals(readConfig.localThreshold(), expectedReadConfig.localThreshold());
        assertEquals(readConfig.readPreferenceConfig(), expectedReadConfig.readPreferenceConfig());
        assertEquals(readConfig.readConcernConfig(), expectedReadConfig.readConcernConfig());
        assertEquals(readConfig.aggregationConfig(), expectedReadConfig.aggregationConfig());
        assertEquals(readConfig.registerSQLHelperFunctions(), expectedReadConfig.registerSQLHelperFunctions());
        assertEquals(readConfig.inferSchemaMapTypesEnabled(), expectedReadConfig.inferSchemaMapTypesEnabled());
        assertEquals(readConfig.inferSchemaMapTypesMinimumKeys(), expectedReadConfig.inferSchemaMapTypesMinimumKeys());
        assertEquals(readConfig.pipelineIncludeNullFilters(), expectedReadConfig.pipelineIncludeNullFilters());
        assertEquals(readConfig.pipelineIncludeFiltersAndProjections(), expectedReadConfig.pipelineIncludeFiltersAndProjections());
    }

    @Test
    public void shouldBeCreatableFromAJavaMap() {
        Map<String, String> options = new HashMap<String, String>();
        options.put(ReadConfig.databaseNameProperty(), "db");
        options.put(ReadConfig.collectionNameProperty(), "collection");
        options.put(ReadConfig.sampleSizeProperty(), "500");
        options.put(ReadConfig.partitionerProperty(), "MongoSamplePartitioner$");
        options.put(ReadConfig.partitionerOptionsProperty() + ".partitionSizeMB", "10");
        options.put(ReadConfig.localThresholdProperty(), "0");
        options.put(ReadConfig.readPreferenceNameProperty(), "secondaryPreferred");
        options.put(ReadConfig.readPreferenceTagSetsProperty(), "[{dc: \"east\", use: \"production\"},{}]");
        options.put(ReadConfig.readConcernLevelProperty(), "majority");
        options.put(ReadConfig.collationProperty(), "{ \"locale\" : \"en\" }");
        options.put(ReadConfig.hintProperty(), "{ \"a\" : 1 }");
        options.put(ReadConfig.registerSQLHelperFunctionsProperty(), "false");
        options.put(ReadConfig.inferSchemaMapTypeEnabledProperty(), "false");
        options.put(ReadConfig.inferSchemaMapTypeMinimumKeysProperty(), "999");
        options.put(ReadConfig.pipelineIncludeNullFiltersProperty(), "false");
        options.put(ReadConfig.pipelineIncludeFiltersAndProjectionsProperty(), "false");

        ReadConfig readConfig = ReadConfig.create(options);
        HashMap<String, String> partitionerOptions = new HashMap<String, String>();
        partitionerOptions.put(ReadConfig.partitionerOptionsProperty() + ".partitionSizeMB", "10");
        ReadConfig expectedReadConfig = ReadConfig.create("db", "collection", null, 500, "MongoSamplePartitioner$",
                partitionerOptions, 0,
                ReadPreference.secondaryPreferred(asList(new TagSet(asList(new Tag("dc", "east"), new Tag("use", "production"))), new TagSet())),
                ReadConcern.MAJORITY, Collation.builder().locale("en").build(), BsonDocument.parse("{a: 1}"), false, false, 999, false, false);

        assertEquals(readConfig.databaseName(), expectedReadConfig.databaseName());
        assertEquals(readConfig.collectionName(), expectedReadConfig.collectionName());
        assertEquals(readConfig.connectionString(), expectedReadConfig.connectionString());
        assertEquals(readConfig.sampleSize(), expectedReadConfig.sampleSize());
        assertEquals(readConfig.partitioner(), expectedReadConfig.partitioner());
        assertEquals(readConfig.partitionerOptions(), expectedReadConfig.partitionerOptions());
        assertEquals(readConfig.localThreshold(), expectedReadConfig.localThreshold());
        assertEquals(readConfig.readPreferenceConfig(), expectedReadConfig.readPreferenceConfig());
        assertEquals(readConfig.readConcernConfig(), expectedReadConfig.readConcernConfig());
        assertEquals(readConfig.aggregationConfig(), expectedReadConfig.aggregationConfig());
        assertEquals(readConfig.registerSQLHelperFunctions(), expectedReadConfig.registerSQLHelperFunctions());
        assertEquals(readConfig.inferSchemaMapTypesEnabled(), expectedReadConfig.inferSchemaMapTypesEnabled());
        assertEquals(readConfig.inferSchemaMapTypesMinimumKeys(), expectedReadConfig.inferSchemaMapTypesMinimumKeys());
        assertEquals(readConfig.pipelineIncludeNullFilters(), expectedReadConfig.pipelineIncludeNullFilters());
        assertEquals(readConfig.pipelineIncludeFiltersAndProjections(), expectedReadConfig.pipelineIncludeFiltersAndProjections());
    }

    @Test
    public void shouldBeCreatableFromAJavaMapAndUseDefaults() {
        Map<String, String> options = new HashMap<String, String>();
        options.put(ReadConfig.databaseNameProperty(), "db");
        options.put(ReadConfig.collectionNameProperty(), "collection");
        options.put(ReadConfig.readPreferenceNameProperty(), "secondaryPreferred");
        options.put(ReadConfig.readConcernLevelProperty(), "majority");

        ReadConfig readConfig = ReadConfig.create(options, ReadConfig.create(getSparkConf().remove("spark.mongodb.input.partitioner")));
        ReadConfig expectedReadConfig = ReadConfig.create("db", "collection", getMongoClientURI(), 1000,
                "DefaultMongoPartitioner$", new HashMap<String, String>(), 15, ReadPreference.secondaryPreferred(),
                ReadConcern.MAJORITY);

        assertEquals(readConfig.databaseName(), expectedReadConfig.databaseName());
        assertEquals(readConfig.collectionName(), expectedReadConfig.collectionName());
        assertEquals(readConfig.connectionString(), expectedReadConfig.connectionString());
        assertEquals(readConfig.sampleSize(), expectedReadConfig.sampleSize());
        assertEquals(readConfig.partitioner(), expectedReadConfig.partitioner());
        assertEquals(readConfig.partitionerOptions(), expectedReadConfig.partitionerOptions());
        assertEquals(readConfig.localThreshold(), expectedReadConfig.localThreshold());
        assertEquals(readConfig.readPreferenceConfig(), expectedReadConfig.readPreferenceConfig());
        assertEquals(readConfig.readConcernConfig(), expectedReadConfig.readConcernConfig());
        assertEquals(readConfig.registerSQLHelperFunctions(), expectedReadConfig.registerSQLHelperFunctions());
        assertEquals(readConfig.inferSchemaMapTypesEnabled(), expectedReadConfig.inferSchemaMapTypesEnabled());
        assertEquals(readConfig.inferSchemaMapTypesMinimumKeys(), expectedReadConfig.inferSchemaMapTypesMinimumKeys());
        assertEquals(readConfig.pipelineIncludeNullFilters(), expectedReadConfig.pipelineIncludeNullFilters());
        assertEquals(readConfig.pipelineIncludeFiltersAndProjections(), expectedReadConfig.pipelineIncludeFiltersAndProjections());
    }
}
