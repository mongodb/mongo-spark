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
package com.mongodb.spark.sql.connector.write;

import static com.mongodb.spark.sql.connector.config.WriteConfig.TRUNCATE_MODE_CONFIG;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.config.WriteConfig;
import com.mongodb.spark.sql.connector.mongodb.MongoSparkConnectorTestCase;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;
import org.junit.jupiter.api.Test;

class WriteAtomicityTest extends MongoSparkConnectorTestCase {

  @Test
  void failedOverwriteKeepsThePreviousCollection() {
    documents().insertOne(new Document("_id", 1).append("v", "original"));
    SparkSession spark = getOrCreateSparkSession();

    assertThrows(SparkException.class, () -> duplicateIdOverwrite(spark, "drop"));

    assertOriginalDocumentRemains();
    assertNoStagingCollections();
  }

  @Test
  void failedTruncateOverwriteKeepsIndexesAndDocuments() {
    documents().drop();
    getDatabase().createCollection(getCollectionName());
    documents().createIndex(new Document("v", 1), new IndexOptions().name("vIndex"));
    documents().insertOne(new Document("_id", 1).append("v", "original"));
    SparkSession spark = getOrCreateSparkSession();

    assertThrows(SparkException.class, () -> duplicateIdOverwrite(spark, "truncate"));

    assertOriginalDocumentRemains();
    List<String> indexes =
        documents().listIndexes().map(index -> index.getString("name")).into(new ArrayList<>());
    assertTrue(indexes.contains("vIndex"), indexes.toString());
    assertNoStagingCollections();
  }

  @Test
  void emptyOverwriteReplacesTheCollection() {
    documents().insertOne(new Document("_id", 1).append("v", "original"));
    SparkSession spark = getOrCreateSparkSession();
    StructType schema =
        new StructType().add("_id", DataTypes.IntegerType).add("v", DataTypes.StringType);
    Dataset<Row> empty = spark.createDataFrame(new ArrayList<Row>(), schema);

    empty
        .write()
        .format("mongodb")
        .mode("overwrite")
        .option(TRUNCATE_MODE_CONFIG, "drop")
        .save();

    assertEquals(0, documents().countDocuments());
    assertNoStagingCollections();
  }

  @Test
  void emptyTruncateOverwriteKeepsIndexes() {
    documents().drop();
    getDatabase().createCollection(getCollectionName());
    documents().createIndex(new Document("v", 1), new IndexOptions().name("vIndex"));
    documents().insertOne(new Document("_id", 1).append("v", "original"));
    SparkSession spark = getOrCreateSparkSession();
    StructType schema =
        new StructType().add("_id", DataTypes.IntegerType).add("v", DataTypes.StringType);

    spark
        .createDataFrame(new ArrayList<Row>(), schema)
        .write()
        .format("mongodb")
        .mode("overwrite")
        .option(TRUNCATE_MODE_CONFIG, "truncate")
        .save();

    assertEquals(0, documents().countDocuments());
    List<String> indexes =
        documents().listIndexes().map(index -> index.getString("name")).into(new ArrayList<>());
    assertTrue(indexes.contains("vIndex"), indexes.toString());
    assertNoStagingCollections();
  }

  @Test
  void retriedAttemptWithoutIdWritesOneDocument() {
    documents().drop();
    SparkSession spark = getOrCreateSparkSession();
    StructType schema = new StructType().add("name", DataTypes.StringType);
    ExpressionEncoder<Row> encoder = ExpressionEncoder.apply(schema);
    InternalRow row = encoder.createSerializer().apply(RowFactory.create("Bilbo"));
    WriteConfig config = MongoConfig.writeConfig(getMongoConfig().getOriginals());

    writeRow(config, schema, row, 0, 11L, -1L, "query-a");
    writeRow(config, schema, row, 0, 22L, -1L, "query-a");
    assertEquals(1, documents().countDocuments());
    assertEquals("Bilbo", documents().find().first().getString("name"));

    writeRow(config, schema, row, 1, 33L, -1L, "query-a");
    assertEquals(2, documents().countDocuments());

    writeRow(config, schema, row, 0, 44L, 1L, "query-a");
    assertEquals(3, documents().countDocuments());
  }

  @Test
  void separateAppendsWithoutIdInsertSeparateDocuments() {
    documents().drop();
    SparkSession spark = getOrCreateSparkSession();
    StructType schema = new StructType().add("name", DataTypes.StringType);
    Dataset<Row> rows = spark.createDataFrame(singletonList(RowFactory.create("Bilbo")), schema);

    rows.write().format("mongodb").mode("append").save();
    rows.write().format("mongodb").mode("append").save();

    assertEquals(2, documents().countDocuments());
    assertNoStagingCollections();
  }

  private void duplicateIdOverwrite(final SparkSession spark, final String truncateMode) {
    StructType schema =
        new StructType().add("_id", DataTypes.IntegerType).add("v", DataTypes.StringType);
    Dataset<Row> rows = spark.createDataFrame(
        asList(RowFactory.create(10, "first-batch"), RowFactory.create(10, "second-batch")),
        schema);
    rows.write()
        .format("mongodb")
        .mode("overwrite")
        .option("operationType", "insert")
        .option("maxBatchSize", "1")
        .option(TRUNCATE_MODE_CONFIG, truncateMode)
        .save();
  }

  private void assertOriginalDocumentRemains() {
    assertEquals(1, documents().countDocuments());
    Document document = documents().find().first();
    assertEquals(1, document.getInteger("_id"));
    assertEquals("original", document.getString("v"));
  }

  private void assertNoStagingCollections() {
    List<String> staging = getDatabase().listCollectionNames().into(new ArrayList<>()).stream()
        .filter(name -> name.startsWith(OverwriteStaging.COLLECTION_PREFIX))
        .collect(Collectors.toList());
    assertTrue(staging.isEmpty(), staging.toString());
  }

  private MongoCollection<Document> documents() {
    return getDatabase().getCollection(getCollectionName());
  }

  private void writeRow(
      final WriteConfig config,
      final StructType schema,
      final InternalRow row,
      final int partitionId,
      final long taskId,
      final long epochId,
      final String queryId) {
    MongoDataWriter writer =
        new MongoDataWriter(partitionId, taskId, schema, config, epochId, queryId);
    try {
      writer.write(row);
      writer.commit();
    } finally {
      writer.close();
    }
  }
}
