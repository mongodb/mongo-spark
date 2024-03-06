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

import static com.mongodb.spark.sql.connector.config.MongoConfig.COMMENT_CONFIG;
import static com.mongodb.spark.sql.connector.interop.JavaScala.asJava;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.TimeSeriesGranularity;
import com.mongodb.client.model.TimeSeriesOptions;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.config.WriteConfig;
import com.mongodb.spark.sql.connector.mongodb.MongoSparkConnectorTestCase;
import com.mongodb.spark.sql.connector.schema.InferSchema;
import com.mongodb.spark.sql.connector.schema.RowToBsonDocumentConverter;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

class MongoBatchTest extends MongoSparkConnectorTestCase {
  private static BsonDocument fromRowDefault(final Row row) {
    return new RowToBsonDocumentConverter(row.schema(), WriteConfig.ConvertJson.FALSE, false)
        .fromRow(row);
  }

  private static final String READ_RESOURCES_HOBBITS_JSON_PATH =
      "src/integrationTest/resources/data/read/hobbits.json";
  private static final String READ_RESOURCES_INFER_SCHEMA_JSON_PATH =
      "src/integrationTest/resources/data/read/infer_schema.json";

  private static final String BSON_DOCUMENT_JSON = "{"
      + "\"_id\": 1, "
      + "\"arrayEmpty\": [], "
      + "\"arraySimple\": [{\"$numberInt\": \"1\"}, {\"$numberInt\": \"2\"}, {\"$numberInt\": \"3\"}], "
      + "\"arrayComplex\": [{\"a\": {\"$numberInt\": \"1\"}}, {\"a\": {\"$numberInt\": \"2\"}}], "
      + "\"arrayMixedTypes\": [{\"$numberInt\": \"1\"}, {\"$numberInt\": \"2\"}, true,"
      + " [{\"$numberInt\": \"1\"}, {\"$numberInt\": \"2\"}, {\"$numberInt\": \"3\"}],"
      + " {\"a\": {\"$numberInt\": \"2\"}}], "
      + "\"arrayComplexMixedTypes\": [{\"a\": {\"$numberInt\": \"1\"}}, {\"a\": \"a\"}], "
      + "\"binary\": {\"$binary\": {\"base64\": \"S2Fma2Egcm9ja3Mh\", \"subType\": \"00\"}}, "
      + "\"boolean\": true, "
      + "\"code\": {\"$code\": \"int i = 0;\"}, "
      + "\"codeWithScope\": {\"$code\": \"int x = y\", \"$scope\": {\"y\": {\"$numberInt\": \"1\"}}}, "
      + "\"dateTime\": {\"$date\": {\"$numberLong\": \"1577836801000\"}}, "
      + "\"decimal128\": {\"$numberDecimal\": \"1.0\"}, "
      + "\"documentEmpty\": {}, "
      + "\"document\": {\"a\": {\"$numberInt\": \"1\"}}, "
      + "\"double\": {\"$numberDouble\": \"62.0\"}, "
      + "\"int32\": {\"$numberInt\": \"42\"}, "
      + "\"int64\": {\"$numberLong\": \"52\"}, "
      + "\"maxKey\": {\"$maxKey\": 1}, "
      + "\"minKey\": {\"$minKey\": 1}, "
      + "\"null\": null, "
      + "\"objectId\": {\"$oid\": \"5f3d1bbde0ca4d2829c91e1d\"}, "
      + "\"regex\": {\"$regularExpression\": {\"pattern\": \"^test.*regex.*xyz$\", \"options\": \"i\"}}, "
      + "\"string\": \"the fox ...\", "
      + "\"symbol\": {\"$symbol\": \"ruby stuff\"}, "
      + "\"timestamp\": {\"$timestamp\": {\"t\": 305419896, \"i\": 5}}, "
      + "\"undefined\": {\"$undefined\": true}"
      + "}";

  private static final String EXPECTED_BSON_DOCUMENT_JSON = "{"
      + "\"_id\": 1, "
      + "\"arrayEmpty\": [], "
      + "\"arraySimple\": [1, 2, 3], "
      + "\"arrayComplex\": [{\"a\": 1}, {\"a\": 2}], "
      + "\"arrayMixedTypes\": [\"1\", \"2\", \"true\", \"[1, 2, 3]\", \"{\\\"a\\\": 2}\"], "
      + "\"arrayComplexMixedTypes\": [{\"a\": \"1\"}, {\"a\": \"a\"}], "
      + "\"binary\": {\"$binary\": {\"base64\": \"S2Fma2Egcm9ja3Mh\", \"subType\": \"00\"}}, "
      + "\"boolean\": true, "
      + "\"code\": \"{\\\"$code\\\": \\\"int i = 0;\\\"}\", "
      + "\"codeWithScope\": \"{\\\"$code\\\": \\\"int x = y\\\", \\\"$scope\\\": {\\\"y\\\": 1}}\", "
      + "\"dateTime\": {\"$date\": {\"$numberLong\": \"1577836801000\"}}, "
      + "\"decimal128\": {\"$numberDecimal\": \"1.0\"}, "
      + "\"documentEmpty\": {}, "
      + "\"document\": {\"a\": {\"$numberInt\": \"1\"}}, "
      + "\"double\": {\"$numberDouble\": \"62.0\"}, "
      + "\"int32\": {\"$numberInt\": \"42\"}, "
      + "\"int64\": {\"$numberLong\": \"52\"}, "
      + "\"maxKey\": \"{\\\"$maxKey\\\": 1}\", "
      + "\"minKey\": \"{\\\"$minKey\\\": 1}\", "
      + "\"null\": null, "
      + "\"objectId\": \"5f3d1bbde0ca4d2829c91e1d\", "
      + "\"regex\": \"{\\\"$regularExpression\\\": {\\\"pattern\\\": \\\"^test.*regex.*xyz$\\\", \\\"options\\\": \\\"i\\\"}}\", "
      + "\"string\": \"the fox ...\", "
      + "\"symbol\": \"ruby stuff\", "
      + "\"timestamp\": {\"$date\": \"1979-09-05T22:51:36Z\"}, "
      + "\"undefined\": \"{\\\"$undefined\\\": true}\""
      + "}";

  @Test
  void testHandlesAllBsonTypes() {
    BsonDocument allTypesDocument = BsonDocument.parse(BSON_DOCUMENT_JSON);
    getCollection().insertOne(allTypesDocument);

    SparkSession spark = getOrCreateSparkSession();
    Row actual = spark.read().format("mongodb").load().first();

    assertEquals(BsonDocument.parse(EXPECTED_BSON_DOCUMENT_JSON), fromRowDefault(actual));
  }

  @Test
  void testWhereFiltersAreConverted() {
    BsonDocument allTypesDocument = BsonDocument.parse(BSON_DOCUMENT_JSON);
    getCollection().insertOne(allTypesDocument);
    SparkSession spark = getOrCreateSparkSession();

    Dataset<Row> dataset = spark.read().format("mongodb").load();

    // Simple types
    Row actual = dataset.where("_id = 1").first();
    assertEquals(BsonDocument.parse(EXPECTED_BSON_DOCUMENT_JSON), fromRowDefault(actual));

    // Casted types
    actual = dataset
        .where("dateTime = cast('2020-01-01T00:00:01.000Z' as timestamp)")
        .first();
    assertEquals(BsonDocument.parse(EXPECTED_BSON_DOCUMENT_JSON), fromRowDefault(actual));

    // Find complex matches
    actual = dataset.where("arraySimple = array(1, 2, 3)").first();
    assertEquals(BsonDocument.parse(EXPECTED_BSON_DOCUMENT_JSON), fromRowDefault(actual));

    // Find nested matches
    actual = dataset.where("document.a = 1").first();
    assertEquals(BsonDocument.parse(EXPECTED_BSON_DOCUMENT_JSON), fromRowDefault(actual));

    // Functional filters - handled by spark
    actual = dataset.filter("array_contains(arraySimple, 2)").first();
    assertEquals(BsonDocument.parse(EXPECTED_BSON_DOCUMENT_JSON), fromRowDefault(actual));
  }

  @Test
  void testReadsAreSupportedWithSchemaSupplied() {
    SparkSession spark = getOrCreateSparkSession();

    List<BsonDocument> collectionData =
        toBsonDocuments(spark.read().textFile(READ_RESOURCES_HOBBITS_JSON_PATH));
    getCollection().insertMany(collectionData);

    StructType schema = createStructType(asList(
        createStructField("_id", DataTypes.IntegerType, false),
        createStructField("age", DataTypes.LongType, true),
        createStructField("name", DataTypes.StringType, true)));

    assertIterableEquals(
        collectionData,
        toBsonDocuments(spark.read().format("mongodb").schema(schema).load().toJSON()));
  }

  @Test
  void testReadsHandleNullsWithSchemaSupplied() {
    SparkSession spark = getOrCreateSparkSession();

    List<BsonDocument> collectionData = singletonList(BsonDocument.parse("{"
        + "_id: 1,"
        + "arrayNull: null,"
        + "arrayContainingNull: [null],"
        + "structNull: null,"
        + "structContainingNull: {A: null},"
        + "mapNull: null,"
        + "mapContainingNull: {A: null},"
        + "}"));
    getCollection().insertMany(collectionData);

    StructType schema = new StructType()
        .add("_id", DataTypes.IntegerType, true)
        .add("arrayNull", DataTypes.createArrayType(DataTypes.StringType, true))
        .add("arrayContainingNull", DataTypes.createArrayType(DataTypes.IntegerType, true))
        .add(
            "structNull",
            new StructType().add("A", DataTypes.createArrayType(DataTypes.IntegerType, true)))
        .add(
            "structContainingNull",
            new StructType().add("A", DataTypes.createArrayType(DataTypes.IntegerType, true)))
        .add("mapNull", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true))
        .add(
            "mapContainingNull",
            DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true));

    Row row = spark.read().format("mongodb").schema(schema).load().first();

    assertAll(
        () -> assertEquals(1, row.getInt(0)),
        () -> assertNull(row.get(1)),
        () -> assertIterableEquals(singletonList(null), row.getList(2)),
        () -> assertNull(row.get(3)),
        () -> {
          Object[] structValues = {null};
          new StructType().add("A", DataTypes.createArrayType(DataTypes.IntegerType, true));
          assertEquals(new GenericRowWithSchema(structValues, schema), row.getStruct(4));
        },
        () -> assertNull(row.get(5)),
        () -> assertEquals(
            new HashMap<String, String>() {
              {
                put("A", null);
              }
            },
            row.getJavaMap(6)));
  }

  @Test
  void testReadsAreSupportedWithSchemaInferred() {
    SparkSession spark = getOrCreateSparkSession();

    String collectionName = "inferredTest";
    List<BsonDocument> collectionData =
        toBsonDocuments(spark.read().textFile(READ_RESOURCES_INFER_SCHEMA_JSON_PATH));
    getDatabase()
        .getCollection(collectionName)
        .withDocumentClass(BsonDocument.class)
        .insertMany(collectionData);

    Dataset<Row> dataSet = spark
        .read()
        .format("mongodb")
        .option(ReadConfig.COLLECTION_NAME_CONFIG, collectionName)
        .load();

    StructType expectedSchema = createStructType(asList(
        createStructField("_id", DataTypes.IntegerType, true, InferSchema.INFERRED_METADATA),
        createStructField("email", DataTypes.StringType, true, InferSchema.INFERRED_METADATA),
        createStructField("misc", DataTypes.StringType, true, InferSchema.INFERRED_METADATA),
        createStructField("name", DataTypes.StringType, true, InferSchema.INFERRED_METADATA)));

    assertEquals(expectedSchema, dataSet.schema());
    assertEquals(20, dataSet.count());

    // Ensure pipeline options are passed to infer schema
    dataSet = spark
        .read()
        .format("mongodb")
        .option(ReadConfig.COLLECTION_NAME_CONFIG, collectionName)
        .option(ReadConfig.AGGREGATION_PIPELINE_CONFIG, "{$match: {email: {$exists: false}}}")
        .load();

    expectedSchema = createStructType(asList(
        createStructField("_id", DataTypes.IntegerType, true, InferSchema.INFERRED_METADATA),
        createStructField("misc", DataTypes.StringType, true, InferSchema.INFERRED_METADATA),
        createStructField("name", DataTypes.StringType, true, InferSchema.INFERRED_METADATA)));

    assertEquals(expectedSchema, dataSet.schema());
    assertEquals(14, dataSet.count());
  }

  @Test
  void testReadsAreSupportedWithFilters() {
    SparkSession spark = getOrCreateSparkSession();

    List<BsonDocument> collectionData =
        toBsonDocuments(spark.read().textFile(READ_RESOURCES_HOBBITS_JSON_PATH));
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

    // IsNotNull
    getCollection().insertOne(BsonDocument.parse("{_id: 11, name: 'Gollum', age: null}"));
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

  @Test
  void testReadsAreSupportedWithHyphenedNamesFilters() {
    SparkSession spark = getOrCreateSparkSession();

    List<BsonDocument> collectionData =
        toBsonDocuments(spark.read().textFile(READ_RESOURCES_HOBBITS_JSON_PATH)).stream()
            .map(d -> d.append("full-name", d.remove("name")).append("actual-age", d.remove("age")))
            .collect(Collectors.toList());
    getCollection().insertMany(collectionData);
    getCollection().insertOne(BsonDocument.parse("{_id: 10, 'full-name': 'Bombur'}"));

    Dataset<Row> ds = spark.read().format("mongodb").load();

    // EqualNullSafe
    assertIterableEquals(
        singletonList("Gandalf"),
        ds.filter(new Column("actual-age").eqNullSafe(1000))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // EqualTo
    assertIterableEquals(
        singletonList("Gandalf"),
        ds.filter(new Column("actual-age").equalTo(1000))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // GreaterThan
    assertIterableEquals(
        asList("Gandalf", "Thorin"),
        ds.filter(new Column("actual-age").gt(178))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // GreaterThanOrEqual
    assertIterableEquals(
        asList("Gandalf", "Thorin", "Balin"),
        ds.filter(new Column("actual-age").geq(178))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // In
    assertIterableEquals(
        asList("Kíli", "Fíli"),
        ds.filter(new Column("full-name").isin("Kíli", "Fíli"))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // IsNull
    assertIterableEquals(
        singletonList("Bombur"),
        ds.filter(new Column("actual-age").isNull())
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // LessThan
    assertIterableEquals(
        asList("Bilbo Baggins", "Kíli"),
        ds.filter(new Column("actual-age").lt(82))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // LessThanOrEqual
    assertIterableEquals(
        asList("Bilbo Baggins", "Kíli", "Fíli"),
        ds.filter(new Column("actual-age").leq(82))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // Not
    assertIterableEquals(
        asList("Gandalf", "Thorin", "Balin", "Kíli", "Dwalin", "Óin", "Glóin", "Fíli"),
        ds.filter(new Column("actual-age").notEqual(50))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // StringContains
    assertIterableEquals(
        asList("Bilbo Baggins", "Thorin", "Balin", "Dwalin", "Óin", "Glóin"),
        ds.filter(new Column("full-name").contains("in"))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // StringEndsWith
    assertIterableEquals(
        asList("Kíli", "Fíli"),
        ds.filter(new Column("full-name").endsWith("li"))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // StringStartsWith
    assertIterableEquals(
        asList("Gandalf", "Glóin"),
        ds.filter(new Column("full-name").startsWith("G"))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());

    // And
    assertIterableEquals(
        singletonList("Gandalf"),
        ds.filter(new Column("full-name").startsWith("G").and(new Column("actual-age").gt(200)))
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());
    // Or
    assertIterableEquals(
        asList("Bilbo Baggins", "Balin", "Kíli", "Fíli", "Bombur"),
        ds.filter(new Column("full-name").startsWith("B").or(new Column("actual-age").lt(150)))
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
        ds.filter(new Column("actual-age").isNotNull())
            .map((MapFunction<Row, String>) r -> r.getString(2), Encoders.STRING())
            .collectAsList());
  }

  @Test
  void testReadsCanFilterNonExistentFields() {
    SparkSession spark = getOrCreateSparkSession();

    List<BsonDocument> collectionData = asList(
        BsonDocument.parse("{a: 1, b: []}"),
        BsonDocument.parse("{a: 2, b: [\"1\"]}"),
        BsonDocument.parse("{a: 3}"),
        BsonDocument.parse("{b: [\"2\"]}"),
        BsonDocument.parse("{b: [\"3\"]}"),
        BsonDocument.parse("{a: 4, b: [\"4\"]}"));
    getCollection().insertMany(collectionData);

    Dataset<Row> ds = spark.read().format("mongodb").load();

    long dataSize = ds.count();
    assertEquals(6, dataSize);

    // a exists
    dataSize = ds.filter("a IS NOT NULL").count();
    assertEquals(4, dataSize);

    // b exists
    dataSize = ds.filter("b IS NOT NULL").count();
    assertEquals(5, dataSize);

    // a & b exists
    dataSize = ds.filter("a IS NOT NULL AND b IS NOT NULL").count();
    assertEquals(3, dataSize);
  }

  @Test
  void testCustomMongoClientFactory() {
    SparkSession spark = getOrCreateSparkSession();

    List<BsonDocument> collectionData =
        toBsonDocuments(spark.read().textFile(READ_RESOURCES_HOBBITS_JSON_PATH));
    getCollection().insertMany(collectionData);

    spark
        .read()
        .format("mongodb")
        .option(
            ReadConfig.READ_PREFIX + ReadConfig.CLIENT_FACTORY_CONFIG,
            "com.mongodb.spark.sql.connector.read.CustomMongoClientFactory")
        .load()
        .collect();

    assertTrue(CustomMongoClientFactory.CALLED.get());
  }

  @Test
  void testReadFromTimeseriesDatabase() {
    assumeTrue(isAtLeastFiveDotZero() && !isSharded());
    SparkSession spark = getOrCreateSparkSession();
    getCollection().drop();

    getDatabase()
        .createCollection(
            getCollectionName(),
            new CreateCollectionOptions()
                .timeSeriesOptions(new TimeSeriesOptions("timestamp")
                    .metaField("metadata")
                    .granularity(TimeSeriesGranularity.HOURS)));

    List<BsonDocument> collectionData = asList(
        BsonDocument.parse(
            "{'timestamp': {'$date': '2021-05-18T00:00:00Z'}, metadata: {sensorId: 5578, type: 'temperature'}, temp: 12}"),
        BsonDocument.parse(
            "{'timestamp': {'$date': '2021-05-18T04:00:00Z'}, metadata: {sensorId: 5578, type: 'temperature'}, temp: 11}"),
        BsonDocument.parse(
            "{'timestamp': {'$date': '2021-05-18T08:00:00Z'}, metadata: {sensorId: 5578, type: 'temperature'}, temp: 11}"),
        BsonDocument.parse(
            "{'timestamp': {'$date': '2021-05-18T12:00:00Z'}, metadata: {sensorId: 5578, type: 'temperature'}, temp: 12}"),
        BsonDocument.parse(
            "{'timestamp': {'$date': '2021-05-18T16:00:00Z'}, metadata: {sensorId: 5578, type: 'temperature'}, temp: 16}"),
        BsonDocument.parse(
            "{'timestamp': {'$date': '2021-05-18T20:00:00Z'}, metadata: {sensorId: 5578, type: 'temperature'}, temp: 15}"),
        BsonDocument.parse(
            "{'timestamp': {'$date': '2021-05-19T00:00:00Z'}, metadata: {sensorId: 5578, type: 'temperature'}, temp: 13}"),
        BsonDocument.parse(
            "{'timestamp': {'$date': '2021-05-19T04:00:00Z'}, metadata: {sensorId: 5578, type: 'temperature'}, temp: 12}"),
        BsonDocument.parse(
            "{'timestamp': {'$date': '2021-05-19T08:00:00Z'}, metadata: {sensorId: 5578, type: 'temperature'}, temp: 11}"),
        BsonDocument.parse(
            "{'timestamp': {'$date': '2021-05-19T12:00:00Z'}, metadata: {sensorId: 5578, type: 'temperature'}, temp: 12}"),
        BsonDocument.parse(
            "{'timestamp': {'$date': '2021-05-19T16:00:00Z'}, metadata: {sensorId: 5578, type: 'temperature'}, temp: 17}"),
        BsonDocument.parse(
            "{'timestamp': {'$date': '2021-05-19T20:00:00Z'}, metadata: {sensorId: 5578, type: 'temperature'}, temp: 12}"));
    getCollection().insertMany(collectionData);

    DataFrameReader dfr = spark.read().format("mongodb");

    String partitioner =
        "com.mongodb.spark.sql.connector.read.partitioner.PaginateBySizePartitioner";
    assertEquals(
        collectionData.size(), dfr.option("partitioner", partitioner).load().count());

    partitioner =
        "com.mongodb.spark.sql.connector.read.partitioner.PaginateIntoPartitionsPartitioner";
    assertEquals(
        collectionData.size(), dfr.option("partitioner", partitioner).load().count());

    partitioner = "com.mongodb.spark.sql.connector.read.partitioner.SamplePartitioner";
    assertEquals(
        collectionData.size(), dfr.option("partitioner", partitioner).load().count());

    partitioner = "com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner";
    assertEquals(
        collectionData.size(), dfr.option("partitioner", partitioner).load().count());
  }

  @Test
  void testReadsLogCommentsInProfilerLogs() {
    SparkSession spark = getOrCreateSparkSession();

    List<BsonDocument> collectionData =
        toBsonDocuments(spark.read().textFile(READ_RESOURCES_HOBBITS_JSON_PATH));
    getCollection().insertMany(collectionData);

    WriteConfig writeConfig = MongoConfig.writeConfig(asJava(spark.initialSessionOptions()))
        .withOption(COMMENT_CONFIG, TEST_COMMENT);

    assertCommentsInProfile(
        () -> {
          StructType schema = createStructType(asList(
              createStructField("_id", DataTypes.IntegerType, false),
              createStructField("age", DataTypes.LongType, true),
              createStructField("name", DataTypes.StringType, true)));
          assertIterableEquals(
              collectionData,
              toBsonDocuments(spark
                  .read()
                  .option(COMMENT_CONFIG, TEST_COMMENT)
                  .format("mongodb")
                  .schema(schema)
                  .load()
                  .toJSON()));
        },
        writeConfig);
  }

  @Test
  void testWritesLogCommentsInProfilerLogs() {
    SparkSession spark = getOrCreateSparkSession();

    List<BsonDocument> collectionData =
        toBsonDocuments(spark.read().textFile(READ_RESOURCES_HOBBITS_JSON_PATH));
    getCollection().insertMany(collectionData);

    WriteConfig writeConfig = MongoConfig.writeConfig(asJava(spark.initialSessionOptions()))
        .withOption(COMMENT_CONFIG, TEST_COMMENT);

    assertCommentsInProfile(
        () -> {
          StructType schema = createStructType(asList(
              createStructField("_id", DataTypes.IntegerType, false),
              createStructField("age", DataTypes.LongType, true),
              createStructField("name", DataTypes.StringType, true)));
          assertIterableEquals(
              collectionData,
              toBsonDocuments(spark
                  .read()
                  .option(COMMENT_CONFIG, TEST_COMMENT)
                  .format("mongodb")
                  .schema(schema)
                  .load()
                  .toJSON()));
        },
        writeConfig);
  }

  private List<BsonDocument> toBsonDocuments(final Dataset<String> dataset) {
    return dataset.toJavaRDD().map(BsonDocument::parse).collect();
  }
}
