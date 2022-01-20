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

package com.mongodb.spark.sql.connector.config;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;

import com.mongodb.spark.sql.connector.exceptions.ConfigException;
import com.mongodb.spark.sql.connector.read.partitioner.Partitioner;
import com.mongodb.spark.sql.connector.read.partitioner.PartitionerHelper;

/**
 * The Read Configuration
 *
 * <p>The {@link MongoConfig} for reads.
 */
public final class ReadConfig extends AbstractMongoConfig {

  private static final long serialVersionUID = 1L;
  /**
   * The partitioner full class name.
   *
   * <p>Partitioners must implement the {@link Partitioner} interface.
   *
   * <p>Configuration: {@value}
   *
   * <p>Default: {@code com.mongodb.spark.sql.connector.read.partitioner.SamplePartitioner}
   */
  public static final String PARTITIONER_CONFIG = "partitioner";

  private static final String PARTITIONER_DEFAULT = PartitionerHelper.DEFAULT_PARTITIONER;

  /**
   * The prefix for specific partitioner based configuration.
   *
   * <p>Any configuration beginning with this prefix will is available via {@link
   * #getPartitionerOptions()}.
   *
   * <p>Configuration: {@value}
   */
  public static final String PARTITIONER_OPTIONS_PREFIX = "partitioner.options.";

  /**
   * The size of the sample of documents from the collection to use when inferring the schema
   *
   * <p>Configuration: {@value}
   *
   * <p>Default: {@value INFER_SCHEMA_SAMPLE_SIZE_DEFAULT}
   */
  public static final String INFER_SCHEMA_SAMPLE_SIZE_CONFIG = "sampleSize";

  private static final int INFER_SCHEMA_SAMPLE_SIZE_DEFAULT = 1000;

  /**
   * Enable Map Types when inferring the schema.
   *
   * <p>If enabled large compatible struct types will be inferred to a {@code MapType} instead.
   *
   * <p>Configuration: {@value}
   *
   * <p>Default: {@value INFER_SCHEMA_MAP_TYPE_ENABLED_DEFAULT}
   */
  public static final String INFER_SCHEMA_MAP_TYPE_ENABLED_CONFIG =
      "sql.inferSchema.mapTypes.enabled";

  private static final boolean INFER_SCHEMA_MAP_TYPE_ENABLED_DEFAULT = true;

  /**
   * The minimum size of a {@code StructType} before its inferred to a {@code MapType} instead.
   *
   * <p>Configuration: {@value}
   *
   * <p>Default: {@value INFER_SCHEMA_MAP_TYPE_MINIMUM_KEY_SIZE_DEFAULT}. Requires {@code
   * INFER_SCHEMA_MAP_TYPE_ENABLED_CONFIG}
   */
  public static final String INFER_SCHEMA_MAP_TYPE_MINIMUM_KEY_SIZE_CONFIG =
      "sql.inferSchema.mapTypes.minimum.key.size";

  private static final int INFER_SCHEMA_MAP_TYPE_MINIMUM_KEY_SIZE_DEFAULT = 250;

  /**
   * Provide a custom aggregation pipeline.
   *
   * <p>Enables a custom aggregation pipeline to applied to the collection before sending data to
   * Spark.
   *
   * <p>When configuring this should either be an extended json representation of a list of
   * documents:
   *
   * <pre>{@code
   * [{"$match": {"closed": false}}, {"$project": {"status": 1, "name": 1, "description": 1}}]
   * }</pre>
   *
   * Or the extended json syntax of a single document:
   *
   * <pre>{@code
   * {"$match": {"closed": false}}
   * }</pre>
   *
   * <p><strong>Note:</strong> Custom aggregation pipelines must work with the partitioner strategy.
   * Some aggregation stages such as "$group" are not suitable for any partitioner that produces
   * more than one partition.
   *
   * <p>Configuration: {@value}
   *
   * <p>Default: no aggregation pipeline.
   */
  public static final String AGGREGATION_PIPELINE_CONFIG = "aggregation.pipeline";

  public static final String AGGREGATION_PIPELINE_DEFAULT = "";

  /**
   * Allow disk use when running the aggregation.
   *
   * <p>Configuration: {@value}
   *
   * <p>Default: {@value AGGREGATION_ALLOW_DISK_USE_DEFAULT} and allows users to disable writing to
   * disk.
   */
  public static final String AGGREGATION_ALLOW_DISK_USE_CONFIG = "aggregation.allowDiskUse";

  private static final boolean AGGREGATION_ALLOW_DISK_USE_DEFAULT = true;

  private final List<BsonDocument> aggregationPipeline;

  /**
   * Construct a new instance
   *
   * @param options the options for configuration
   */
  ReadConfig(final Map<String, String> options) {
    super(options, UsageMode.READ);
    aggregationPipeline = generateAggregationPipeline();
  }

  @Override
  public ReadConfig withOptions(final Map<String, String> options) {
    if (options.isEmpty()) {
      return this;
    }
    return new ReadConfig(withOverrides(READ_PREFIX, options));
  }

  /** @return the configured infer sample size */
  public int getInferSchemaSampleSize() {
    return getInt(INFER_SCHEMA_SAMPLE_SIZE_CONFIG, INFER_SCHEMA_SAMPLE_SIZE_DEFAULT);
  }

  /** @return the configured infer sample size */
  public boolean inferSchemaMapType() {
    return getBoolean(INFER_SCHEMA_MAP_TYPE_ENABLED_CONFIG, INFER_SCHEMA_MAP_TYPE_ENABLED_DEFAULT);
  }

  /** @return the configured infer sample size */
  public int getInferSchemaMapTypeMinimumKeySize() {
    return getInt(
        INFER_SCHEMA_MAP_TYPE_MINIMUM_KEY_SIZE_CONFIG,
        INFER_SCHEMA_MAP_TYPE_MINIMUM_KEY_SIZE_DEFAULT);
  }

  /** @return the partitioner class name */
  public Partitioner getPartitioner() {
    return ClassHelper.createInstance(
        PARTITIONER_CONFIG,
        getOrDefault(PARTITIONER_CONFIG, PARTITIONER_DEFAULT),
        Partitioner.class,
        this);
  }

  /** @return any partitioner configuration */
  public MongoConfig getPartitionerOptions() {
    return subConfiguration(PARTITIONER_OPTIONS_PREFIX);
  }

  /** @return the aggregation pipeline to filter the collection with */
  public List<BsonDocument> getAggregationPipeline() {
    return aggregationPipeline;
  }

  /** @return the aggregation allow disk use value */
  public boolean getAggregationAllowDiskUse() {
    return getBoolean(AGGREGATION_ALLOW_DISK_USE_CONFIG, AGGREGATION_ALLOW_DISK_USE_DEFAULT);
  }

    /**
     * Handles either a single stage of a pipeline (eg. a single document) or multiple stages (eg. an array of documents).
     *
     * @return the aggregation pipeline
     * @throws ConfigException if the user provided input is invalid
     */
  private List<BsonDocument> generateAggregationPipeline() {
    String pipelineJson = getOrDefault(AGGREGATION_PIPELINE_CONFIG, AGGREGATION_PIPELINE_DEFAULT);
    if (pipelineJson.isEmpty()) {
      return emptyList();
    }
    BsonValue pipeline = BsonDocument.parse(format("{pipeline: %s}", pipelineJson)).get("pipeline");
    switch (pipeline.getBsonType()) {
      case ARRAY:
        BsonArray bsonValues = pipeline.asArray();
        if (bsonValues.isEmpty()) {
          return emptyList();
        } else if (bsonValues.stream().anyMatch(b -> b.getBsonType() != BsonType.DOCUMENT)) {
          throw new ConfigException("Invalid aggregation pipeline: " + pipelineJson);
        }
        return unmodifiableList(
            bsonValues.stream().map(BsonValue::asDocument).collect(Collectors.toList()));
      case DOCUMENT:
        return singletonList(pipeline.asDocument());
      default:
        throw new ConfigException("Invalid aggregation pipeline: " + pipelineJson);
    }
  }
}
