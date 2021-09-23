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

import static java.util.Arrays.asList;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.Nullable;

import com.mongodb.MongoNamespace;

import com.mongodb.spark.sql.connector.write.MongoBatchWrite;

/**
 * Represents a MongoDB Collection.
 *
 * <p>Implements {@link SupportsWrite} for writing to a MongoDB collection
 */
public class MongoTable implements Table, SupportsWrite {

  private static final Set<TableCapability> TABLE_CAPABILITY_SET =
      new HashSet<>(
          asList(
              TableCapability.BATCH_WRITE,
              TableCapability.STREAMING_WRITE,
              TableCapability.TRUNCATE,
              TableCapability.ACCEPT_ANY_SCHEMA));

  private final MongoNamespace mongoNamespace;
  private final StructType schema;
  private final Map<String, String> options;

  /**
   * Constructs a new instance
   *
   * @param mongoNamespace the namespace
   * @param schema the schema or null
   * @param options the options related to the table
   */
  public MongoTable(
      final MongoNamespace mongoNamespace,
      @Nullable final StructType schema,
      final Map<String, String> options) {
    this.mongoNamespace = mongoNamespace;
    this.schema = schema;
    this.options = options;
  }

  /**
   * Returns a {@link WriteBuilder} which can be used to create {@link MongoBatchWrite}. Spark will
   * call this method to configure each data source write.
   *
   * @param info the logical write info
   */
  @Override
  public WriteBuilder newWriteBuilder(final LogicalWriteInfo info) {
    return null;
  }

  /**
   * A name to identify this table. Implementations should provide a meaningful name, like the
   * database and table name from catalog, or the location of files for this table.
   */
  @Override
  public String name() {
    return mongoNamespace.getFullName();
  }

  /**
   * Returns the schema of this table. If the table is not readable and doesn't have a schema, an
   * empty schema can be returned here.
   */
  @Override
  public StructType schema() {
    if (schema != null) {
      return schema;
    }
    // TODO - SPARK-298 infer schema
    return new StructType();
  }

  /** Returns the set of capabilities for this table. */
  @Override
  public Set<TableCapability> capabilities() {
    return TABLE_CAPABILITY_SET;
  }

  @Override
  public String toString() {
    return "MongoTable{"
        + "mongoNamespace="
        + mongoNamespace
        + ", schema="
        + schema
        + ", options="
        + options
        + '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final MongoTable that = (MongoTable) o;
    return Objects.equals(mongoNamespace, that.mongoNamespace)
        && Objects.equals(schema, that.schema)
        && Objects.equals(options, that.options);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mongoNamespace, schema, options);
  }
}
