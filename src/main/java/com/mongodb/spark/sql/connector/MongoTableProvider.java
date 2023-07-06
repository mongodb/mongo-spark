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

package com.mongodb.spark.sql.connector;

import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.schema.InferSchema;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * The MongoDB collection provider
 *
 * <p>Note that: TableProvider can only apply data operations to existing tables, like read, append,
 * delete, and overwrite. It does not support the operations that require metadata changes, like
 * create/drop tables. // TODO support table creation and dropping on write.
 *
 * <p>The major responsibility of this interface is to return a {@link MongoTable} for read/write.
 *
 * <p>Also registers a shortname for use via the services api: {@code
 * spark.read().format("mongodb").load(); }
 */
public final class MongoTableProvider implements TableProvider, DataSourceRegister {

  /** Construct a new instance */
  public MongoTableProvider() {}

  @Override
  public String shortName() {
    return "mongodb";
  }

  /**
   * Infer the schema of the table identified by the given options.
   *
   * @param options an immutable case-insensitive string-to-string map that can identify a table,
   *     e.g. file path, Kafka topic name, etc.
   */
  @Override
  public StructType inferSchema(final CaseInsensitiveStringMap options) {
    return InferSchema.inferSchema(options);
  }

  /**
   * Return a {@link Table} instance with the specified table schema, partitioning and properties to
   * do read/write. The returned table should report the same schema and partitioning with the
   * specified ones, or Spark may fail the operation.
   *
   * @param schema The specified table schema.
   * @param partitioning The specified table partitioning.
   * @param properties The specified table properties. It's case preserving (contains exactly what
   *     users specified) and implementations are free to use it case sensitively or insensitively.
   *     It should be able to identify a table, e.g. file path, Kafka topic name, etc.
   */
  @Override
  public Table getTable(
      final StructType schema,
      final Transform[] partitioning,
      final Map<String, String> properties) {
    return new MongoTable(schema, partitioning, MongoConfig.createConfig(properties));
  }

  /**
   * Returns true if the source has the ability of accepting external table metadata when getting
   * tables. The external table metadata includes: 1. For table reader: user-specified schema from
   * `DataFrameReader`/`DataStreamReader` and schema/partitioning stored in Spark catalog. 2. For
   * table writer: the schema of the input `Dataframe` of `DataframeWriter`/`DataStreamWriter`.
   */
  @Override
  public boolean supportsExternalMetadata() {
    return true;
  }
}
