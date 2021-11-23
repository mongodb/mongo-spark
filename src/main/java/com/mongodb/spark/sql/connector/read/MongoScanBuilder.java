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

import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.mongodb.spark.sql.connector.config.ReadConfig;

/** A builder for a {@link MongoScan}. */
public class MongoScanBuilder
    implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns {

  private final StructType schema;
  private final ReadConfig readConfig;
  private final boolean isCaseSensitive;

  private Filter[] pushedFilters = new Filter[0];
  private StructType prunedSchema;

  /**
   * Construct a new instance
   *
   * @param schema the schema to use for the read
   * @param readConfig the configuration for the read
   */
  public MongoScanBuilder(final StructType schema, final ReadConfig readConfig) {
    this.schema = schema;
    this.readConfig = readConfig;
    this.prunedSchema = schema;
    this.isCaseSensitive =
        SparkSession.getActiveSession()
            .map(s -> s.sessionState().conf().caseSensitiveAnalysis())
            .getOrElse(() -> false);
  }

  /** @return the {@link MongoScan} for the configured scan */
  @Override
  public Scan build() {
    return new MongoScan(prunedSchema, readConfig);
  }

  /**
   * Pushes down filters to the MongoDB aggregation framework.
   *
   * @return any filters that still need to be applied by Spark.
   */
  @Override
  public Filter[] pushFilters(final Filter[] filters) {
    // TODO - SPARK-316
    return filters;
  }

  /** Returns the filters that have been used in creating an aggregation pipeline */
  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  /**
   * Prunes any top level fields.
   *
   * @param requiredSchema the required schema containing only the required top level fields
   */
  @Override
  public void pruneColumns(final StructType requiredSchema) {
    Set<String> requiredColumns =
        Arrays.stream(requiredSchema.fields()).map(this::getColumnName).collect(Collectors.toSet());
    StructField[] fields =
        Arrays.stream(schema.fields())
            .filter(f -> requiredColumns.contains(getColumnName(f)))
            .toArray(StructField[]::new);
    prunedSchema = new StructType(fields);
  }

  private String getColumnName(final StructField field) {
    if (isCaseSensitive) {
      return field.name().toLowerCase(Locale.ROOT);
    }
    return field.name();
  }
}
