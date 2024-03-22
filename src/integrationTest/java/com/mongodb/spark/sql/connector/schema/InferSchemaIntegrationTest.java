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
package com.mongodb.spark.sql.connector.schema;

import static com.mongodb.assertions.Assertions.fail;
import static java.lang.String.join;
import static java.util.Arrays.asList;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.sql.connector.config.CollectionsConfig;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.mongodb.MongoSparkConnectorTestCase;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class InferSchemaIntegrationTest extends MongoSparkConnectorTestCase {
  @ParameterizedTest
  @ValueSource(strings = {"SINGLE", "MULTIPLE", "ALL"})
  void inferSchema(final String collectionsConfigModeStr) {
    CollectionsConfig.Type collectionsConfigType =
        CollectionsConfig.Type.valueOf(collectionsConfigModeStr);
    String collectionNamePrefix = "inferSchemaFromMultipleCollections_" + collectionsConfigType;
    String collectionName1 = collectionNamePrefix + "_1";
    String collectionName2 = collectionNamePrefix + "_2";
    String collectionName3 = collectionNamePrefix + "_3";
    StructField id =
        createStructField("_id", DataTypes.StringType, true, InferSchema.INFERRED_METADATA);
    StructField a =
        createStructField("a", DataTypes.IntegerType, true, InferSchema.INFERRED_METADATA);
    StructField b =
        createStructField("b", DataTypes.StringType, true, InferSchema.INFERRED_METADATA);
    StructField c =
        createStructField("c", DataTypes.BooleanType, true, InferSchema.INFERRED_METADATA);
    StructType expectedSchema;
    String collectionNameOptionValue;
    switch (collectionsConfigType) {
      case SINGLE:
        expectedSchema = createStructType(asList(id, a));
        collectionNameOptionValue = collectionName1;
        break;
      case MULTIPLE:
        expectedSchema = createStructType(asList(id, a, c));
        collectionNameOptionValue = join(",", collectionName1, collectionName3);
        break;
      case ALL:
        expectedSchema = createStructType(asList(id, a, b, c));
        collectionNameOptionValue = "*";
        break;
      default:
        throw fail();
    }
    ReadConfig readConfig = getMongoConfig()
        .withOption(
            ReadConfig.READ_PREFIX + ReadConfig.COLLECTION_NAME_CONFIG, collectionNameOptionValue)
        .toReadConfig();
    readConfig.withClient(client -> {
      MongoDatabase database = client.getDatabase(readConfig.getDatabaseName());
      database
          .getCollection(collectionName1, BsonDocument.class)
          .insertOne(new BsonDocument("a", new BsonInt32(0)));
      database
          .getCollection(collectionName2, BsonDocument.class)
          .insertOne(new BsonDocument("b", new BsonString("")));
      database
          .getCollection(collectionName3, BsonDocument.class)
          .insertOne(new BsonDocument("c", BsonBoolean.FALSE));
      return null;
    });
    StructType actualSchema =
        InferSchema.inferSchema(new CaseInsensitiveStringMap(readConfig.getOptions()));
    assertEquals(expectedSchema, actualSchema);
  }
}
