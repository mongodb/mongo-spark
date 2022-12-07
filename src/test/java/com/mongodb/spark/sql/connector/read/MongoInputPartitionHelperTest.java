/*
 * Copyright 2008-present MongoDB, Inc.
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
package com.mongodb.spark.sql.connector.read;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import org.bson.BsonDocument;

import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.read.partitioner.Partitioner;
import com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner;

public class MongoInputPartitionHelperTest {

  private static final StructType SCHEMA =
      createStructType(
          asList(
              createStructField("a", DataTypes.IntegerType, false),
              createStructField("b", DataTypes.StringType, false),
              createStructField("c", DataTypes.createArrayType(DataTypes.StringType), true)));
  private static final StructType EMPTY_SCHEMA = createStructType(emptyList());
  private static final List<BsonDocument> EMPTY_PIPELINE = emptyList();

  private static final List<BsonDocument> READ_CONFIG_PIPELINE =
      singletonList(BsonDocument.parse("{$match: {isValid: true}}"));

  private static final List<BsonDocument> EXPECTED_PROJECT_PIPELINE =
      singletonList(BsonDocument.parse("{$project: {a: 1, b: 1, c: 1}}"));

  private static final List<BsonDocument> EXPECTED_MERGED_PROJECT_PIPELINE =
      asList(READ_CONFIG_PIPELINE.get(0), EXPECTED_PROJECT_PIPELINE.get(0));
  private static final List<BsonDocument> EXPECTED_FULL_DOCUMENT_PROJECT_PIPELINE =
      singletonList(
          BsonDocument.parse(
              "{$project: {'fullDocument.a': 1, 'fullDocument.b': 1, 'fullDocument.c': 1}}"));

  private static final List<BsonDocument>
      EXPECTED_MERGED_FULL_DOCUMENT_FILTER_AND_PROJECT_PIPELINE =
          asList(READ_CONFIG_PIPELINE.get(0), EXPECTED_FULL_DOCUMENT_PROJECT_PIPELINE.get(0));

  private static final ReadConfig READ_CONFIG;
  private static final ReadConfig READ_CONFIG_WITH_PIPELINE;

  static {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(
        MongoConfig.PREFIX + MongoConfig.CONNECTION_STRING_CONFIG, "mongodb://localhost:27017");
    configMap.put(MongoConfig.PREFIX + MongoConfig.DATABASE_NAME_CONFIG, "db");
    configMap.put(MongoConfig.PREFIX + MongoConfig.COLLECTION_NAME_CONFIG, "coll");
    configMap.put(
        ReadConfig.READ_PREFIX + ReadConfig.PARTITIONER_CONFIG,
        SinglePartitionPartitioner.class.getName());

    READ_CONFIG = MongoConfig.readConfig(configMap);
    READ_CONFIG_WITH_PIPELINE =
        READ_CONFIG.withOption(
            ReadConfig.AGGREGATION_PIPELINE_CONFIG, "[{$match: {isValid: true}}]");
  }

  @Test
  public void generatePipelineTest() {
    assertAll(
        () ->
            assertEquals(
                EXPECTED_PROJECT_PIPELINE,
                MongoInputPartitionHelper.generatePipeline(SCHEMA, READ_CONFIG)),
        () ->
            assertEquals(
                EXPECTED_MERGED_PROJECT_PIPELINE,
                MongoInputPartitionHelper.generatePipeline(SCHEMA, READ_CONFIG_WITH_PIPELINE)),
        () ->
            assertEquals(
                EMPTY_PIPELINE,
                MongoInputPartitionHelper.generatePipeline(EMPTY_SCHEMA, READ_CONFIG)),
        () ->
            assertEquals(
                READ_CONFIG_PIPELINE,
                MongoInputPartitionHelper.generatePipeline(
                    EMPTY_SCHEMA, READ_CONFIG_WITH_PIPELINE)));
  }

  @Test
  public void generatePipelinePublishFullDocumentTest() {
    assertAll(
        () ->
            assertEquals(
                EXPECTED_FULL_DOCUMENT_PROJECT_PIPELINE,
                MongoInputPartitionHelper.generatePipeline(
                    SCHEMA,
                    READ_CONFIG.withOption(
                        ReadConfig.STREAM_PUBLISH_FULL_DOCUMENT_ONLY_CONFIG, "true"))),
        () ->
            assertEquals(
                EXPECTED_MERGED_FULL_DOCUMENT_FILTER_AND_PROJECT_PIPELINE,
                MongoInputPartitionHelper.generatePipeline(
                    SCHEMA,
                    READ_CONFIG_WITH_PIPELINE.withOption(
                        ReadConfig.STREAM_PUBLISH_FULL_DOCUMENT_ONLY_CONFIG, "true"))));
  }

  @Test
  public void generateMongoBatchPartitions() {
    Map<String, String> partitionerOptions = new HashMap<>();
    partitionerOptions.put(
        ReadConfig.PARTITIONER_CONFIG, SingleNoPreferredLocationsPartitioner.class.getName());

    assertAll(
        () ->
            assertEquals(
                EXPECTED_PROJECT_PIPELINE,
                MongoInputPartitionHelper.generateMongoBatchPartitions(
                    SCHEMA, READ_CONFIG.withOptions(partitionerOptions))[0]
                    .getPipeline()),
        () ->
            assertEquals(
                EXPECTED_MERGED_PROJECT_PIPELINE,
                MongoInputPartitionHelper.generateMongoBatchPartitions(
                    SCHEMA, READ_CONFIG_WITH_PIPELINE.withOptions(partitionerOptions))[0]
                    .getPipeline()),
        () ->
            assertEquals(
                EMPTY_PIPELINE,
                MongoInputPartitionHelper.generateMongoBatchPartitions(
                    EMPTY_SCHEMA, READ_CONFIG.withOptions(partitionerOptions))[0]
                    .getPipeline()),
        () ->
            assertEquals(
                READ_CONFIG_PIPELINE,
                MongoInputPartitionHelper.generateMongoBatchPartitions(
                    EMPTY_SCHEMA, READ_CONFIG_WITH_PIPELINE.withOptions(partitionerOptions))[0]
                    .getPipeline()));
  }

  public static class SingleNoPreferredLocationsPartitioner implements Partitioner {

    @Override
    public List<MongoInputPartition> generatePartitions(final ReadConfig readConfig) {
      return singletonList(
          new MongoInputPartition(0, readConfig.getAggregationPipeline(), emptyList()));
    }
  }
}
