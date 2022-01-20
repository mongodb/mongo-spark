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

import java.util.HashMap;
import java.util.Map;

/** A simple implementation of MongoConfig with no set the use case */
class SimpleMongoConfig implements MongoConfig {
  private static final long serialVersionUID = 1L;

  private final Map<String, String> originals;
  private final Map<String, String> options;

  SimpleMongoConfig(final Map<String, String> originals, final Map<String, String> options) {
    this.originals = originals;
    this.options = options;
  }

  /** @return the options for this MongoConfig instance */
  @Override
  public Map<String, String> getOptions() {
    return options;
  }

  /**
   * Return a {@link MongoConfig} instance with the extra options applied.
   *
   * <p>Existing configurations may be overwritten by the new options.
   *
   * @param options the context specific options.
   * @return a new MongoConfig
   */
  @Override
  public MongoConfig withOptions(final Map<String, String> options) {
    HashMap<String, String> mergedOptions = new HashMap<>(getOptions());
    mergedOptions.putAll(options);
    return new SimpleMongoConfig(originals, mergedOptions);
  }

  /** @return the original options for this MongoConfig instance */
  @Override
  public Map<String, String> getOriginals() {
    return originals;
  }
}
