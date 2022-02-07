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
import static java.util.Collections.singletonList;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import org.bson.BsonDocument;

import com.mongodb.spark.sql.connector.mongodb.MongoSparkConnectorTestCase;

class MongoSparkConnectorReadTest extends MongoSparkConnectorTestCase {
  private static final String READ_RESOURCES_JSON_PATH =
      "src/integrationTest/resources/data/read/*.json";

  @Test
  void testReadsAreSupportedWithSchemaSupplied() {
    SparkSession spark = getOrCreateSparkSession();

    List<BsonDocument> collectionData =
        toBsonDocuments(spark.read().textFile(READ_RESOURCES_JSON_PATH));
    getCollection().insertMany(collectionData);

    StructType schema =
        createStructType(
            asList(
                createStructField("_id", DataTypes.IntegerType, false),
                createStructField("age", DataTypes.LongType, true),
                createStructField("name", DataTypes.StringType, true)));

    assertIterableEquals(
        collectionData,
        toBsonDocuments(spark.read().format("mongodb").schema(schema).load().toJSON()));
  }

  @Test
  void testReadsAreSupportedWithSchemaInferred() {
    SparkSession spark = getOrCreateSparkSession();

    List<BsonDocument> collectionData =
        toBsonDocuments(spark.read().textFile(READ_RESOURCES_JSON_PATH));
    getCollection().insertMany(collectionData);

    assertIterableEquals(
        collectionData, toBsonDocuments(spark.read().format("mongodb").load().toJSON()));
  }

  @Test
  void testReadsAreSupportedWithFilters() {
    SparkSession spark = getOrCreateSparkSession();

    List<BsonDocument> collectionData =
        toBsonDocuments(spark.read().textFile(READ_RESOURCES_JSON_PATH));
    getCollection().insertMany(collectionData);
    getCollection().insertOne(BsonDocument.parse("{_id: 10, name: 'Bombur'}"));

    Dataset<Row> ds = spark.read().format("mongodb").load();

    // EqualNullSafe
    assertIterableEquals(
        singletonList("Gandalf"),
        ds.filter(new Column("age").eqNullSafe(1000))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // EqualTo
    assertIterableEquals(
        singletonList("Gandalf"),
        ds.filter(new Column("age").equalTo(1000))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // GreaterThan
    assertIterableEquals(
        asList("Gandalf", "Thorin"),
        ds.filter(new Column("age").gt(178))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // GreaterThanOrEqual
    assertIterableEquals(
        asList("Gandalf", "Thorin", "Balin"),
        ds.filter(new Column("age").geq(178))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // In
    assertIterableEquals(
        asList("Kíli", "Fíli"),
        ds.filter(new Column("name").isin("Kíli", "Fíli"))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // IsNull
    assertIterableEquals(
        singletonList("Bombur"),
        ds.filter(new Column("age").isNull())
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // LessThan
    assertIterableEquals(
        asList("Bilbo Baggins", "Kíli"),
        ds.filter(new Column("age").lt(82))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // LessThanOrEqual
    assertIterableEquals(
        asList("Bilbo Baggins", "Kíli", "Fíli"),
        ds.filter(new Column("age").leq(82))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // Not
    assertIterableEquals(
        asList("Gandalf", "Thorin", "Balin", "Kíli", "Dwalin", "Óin", "Glóin", "Fíli"),
        ds.filter(new Column("age").notEqual(50))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // StringContains
    assertIterableEquals(
        asList("Bilbo Baggins", "Thorin", "Balin", "Dwalin", "Óin", "Glóin"),
        ds.filter(new Column("name").contains("in"))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // StringEndsWith
    assertIterableEquals(
        asList("Kíli", "Fíli"),
        ds.filter(new Column("name").endsWith("li"))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // StringStartsWith
    assertIterableEquals(
        asList("Gandalf", "Glóin"),
        ds.filter(new Column("name").startsWith("G"))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // And
    assertIterableEquals(
        singletonList("Gandalf"),
        ds.filter(new Column("name").startsWith("G").and(new Column("age").gt(200)))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());
    // Or
    assertIterableEquals(
        asList("Bilbo Baggins", "Balin", "Kíli", "Fíli", "Bombur"),
        ds.filter(new Column("name").startsWith("B").or(new Column("age").lt(150)))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // IsNotNull - filter handled by Spark alone
    assertIterableEquals(
        asList(
            "Bilbo Baggins",
            "Gandalf",
            "Thorin",
            "Balin",
            "Kíli",
            "Dwalin",
            "Óin",
            "Glóin",
            "Fíli"),
        ds.filter(new Column("age").isNotNull())
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());
  }

  private List<BsonDocument> toBsonDocuments(final Dataset<String> dataset) {
    return dataset.toJavaRDD().map(BsonDocument::parse).collect();
  }
}
