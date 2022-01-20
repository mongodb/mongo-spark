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

import java.util.Map;

/**
 * The Read Configuration
 *
 * <p>The {@link MongoConfig} for reads.
 */
public final class ReadConfig extends AbstractMongoConfig {

  private static final long serialVersionUID = 1L;

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
   * Construct a new instance
   *
   * @param options the options for configuration
   */
  ReadConfig(final Map<String, String> options) {
    super(options, UsageMode.READ);
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
}
