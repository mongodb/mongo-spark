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
import java.util.Objects;
import java.util.stream.Collectors;

/** A simple implementation of MongoConfig with no set the use case */
class SimpleMongoConfig implements MongoConfig {
  private static final long serialVersionUID = 1L;

  private final Map<String, String> options;

  SimpleMongoConfig(final Map<String, String> options) {
    this.options = options;
  }

  @Override
  public Map<String, String> getOptions() {
    return options;
  }

  @Override
  public MongoConfig withOption(final String key, final String value) {
    HashMap<String, String> mergedOptions = new HashMap<>(getOptions());
    mergedOptions.put(key, value);
    return new SimpleMongoConfig(mergedOptions);
  }

  @Override
  public MongoConfig withOptions(final Map<String, String> options) {
    HashMap<String, String> mergedOptions = new HashMap<>(getOptions());
    mergedOptions.putAll(options);
    return new SimpleMongoConfig(mergedOptions);
  }

  @Override
  public Map<String, String> getOriginals() {
    return options;
  }

  @Override
  public String toString() {
    String cleanedOptions = options.entrySet().stream()
        .map(e -> {
          String value = e.getValue();
          if (e.getKey().contains(CONNECTION_STRING_CONFIG)) {
            value = "<hidden>";
          }
          return e.getKey() + "=" + value;
        })
        .collect(Collectors.joining(", "));
    return "MongoConfig{options=" + cleanedOptions + ", usageMode=NotSet}";
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SimpleMongoConfig that = (SimpleMongoConfig) o;
    return Objects.equals(options, that.options);
  }

  @Override
  public int hashCode() {
    return Objects.hash(options);
  }
}
