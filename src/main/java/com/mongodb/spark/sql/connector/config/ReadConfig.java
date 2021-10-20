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
   * Construct a new instance
   *
   * @param options the options for configuration
   */
  ReadConfig(final Map<String, String> options) {
    super(options, UsageMode.READ);
  }

  /**
   * Create a new {@link ReadConfig} instance with the extra options.
   *
   * <p>Existing configurations may be overwritten by the new options.
   *
   * @param overrides the context specific options.
   * @return a new ReadConfig
   */
  @Override
  public ReadConfig withOptions(final Map<String, String> overrides) {
    return new ReadConfig(withOverrides(overrides));
  }
}
