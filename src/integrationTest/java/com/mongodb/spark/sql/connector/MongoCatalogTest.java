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

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;

import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.mongodb.MongoSparkConnectorTestCase;

public class MongoCatalogTest extends MongoSparkConnectorTestCase {
  private static final MongoCatalog MONGO_CATALOG = new MongoCatalog();
  private static final String TEST_DATABASE_NAME = "SPARK_CONNECTOR_TEST";
  private static final String TEST_DATABASE_NAME_ALT = "SPARK_CONNECTOR_TEST_ALT";
  private static final String TEST_COLLECTION_NAME = "COLL";
  private static final Identifier TEST_IDENTIFIER =
      createIdentifier(TEST_DATABASE_NAME, TEST_COLLECTION_NAME);
  private static final Identifier TEST_IDENTIFIER_ALT =
      createIdentifier(TEST_DATABASE_NAME_ALT, TEST_COLLECTION_NAME);
  private static final String NOT_INITIALIZED = "The MongoCatalog is has not been initialized";

  @AfterEach
  void afterEach() {
    MONGO_CATALOG.reset(
        () ->
            MONGO_CATALOG
                .getWriteConfig()
                .doWithClient(
                    c ->
                        asList(TEST_DATABASE_NAME, TEST_DATABASE_NAME_ALT)
                            .forEach(db -> c.getDatabase(db).drop())));
  }

  @Test
  void initializeTest() {
    assertDoesNotThrow(
        () -> MONGO_CATALOG.initialize(TEST_DATABASE_NAME, getConnectionProviderOptions()));
    assertThrows(
        IllegalStateException.class,
        () -> MONGO_CATALOG.initialize(TEST_DATABASE_NAME, getConnectionProviderOptions()),
        "The MongoCatalog has already been initialized.");
  }

  @Test
  void nameTest() {
    assertThrows(IllegalStateException.class, MONGO_CATALOG::name, NOT_INITIALIZED);

    MONGO_CATALOG.initialize(TEST_DATABASE_NAME, getConnectionProviderOptions());
    assertEquals(TEST_DATABASE_NAME, MONGO_CATALOG.name());
  }

  @Test
  void listNamespacesTest() throws NoSuchNamespaceException {
    assertThrows(IllegalStateException.class, MONGO_CATALOG::listNamespaces, NOT_INITIALIZED);
    assertThrows(
        IllegalStateException.class,
        () -> MONGO_CATALOG.listNamespaces(TEST_IDENTIFIER.namespace()));

    MONGO_CATALOG.initialize(TEST_DATABASE_NAME, getConnectionProviderOptions());
    createCollection(TEST_IDENTIFIER);

    assertTrue(
        Arrays.stream(MONGO_CATALOG.listNamespaces())
            .anyMatch(s -> Arrays.equals(s, TEST_IDENTIFIER.namespace())));

    assertThrows(
        NoSuchNamespaceException.class,
        () -> MONGO_CATALOG.listNamespaces(TEST_IDENTIFIER_ALT.namespace()));
    assertEquals(0, MONGO_CATALOG.listNamespaces(TEST_IDENTIFIER.namespace()).length);

    assertArrayEquals(MONGO_CATALOG.listNamespaces(), MONGO_CATALOG.listNamespaces(new String[0]));
  }

  @Test
  void namespaceExistsTest() {
    assertThrows(
        IllegalStateException.class,
        () -> MONGO_CATALOG.namespaceExists(TEST_IDENTIFIER.namespace()),
        NOT_INITIALIZED);

    MONGO_CATALOG.initialize(TEST_DATABASE_NAME, getConnectionProviderOptions());
    assertFalse(MONGO_CATALOG.namespaceExists(TEST_IDENTIFIER.namespace()));

    createCollection(TEST_IDENTIFIER);
    assertTrue(MONGO_CATALOG.namespaceExists(TEST_IDENTIFIER.namespace()));
  }

  @Test
  void loadNamespaceMetadataTest() throws NoSuchNamespaceException {
    assertThrows(
        IllegalStateException.class,
        () -> MONGO_CATALOG.loadNamespaceMetadata(TEST_IDENTIFIER.namespace()),
        NOT_INITIALIZED);

    MONGO_CATALOG.initialize(TEST_DATABASE_NAME, getConnectionProviderOptions());
    assertThrows(
        NoSuchNamespaceException.class,
        () -> MONGO_CATALOG.loadNamespaceMetadata(TEST_IDENTIFIER_ALT.namespace()));

    createCollection(TEST_IDENTIFIER);
    assertEquals(emptyMap(), MONGO_CATALOG.loadNamespaceMetadata(TEST_IDENTIFIER.namespace()));
  }

  @Test
  void createNamespaceTest() {
    assertThrows(
        IllegalStateException.class,
        () -> MONGO_CATALOG.createNamespace(TEST_IDENTIFIER.namespace(), emptyMap()),
        NOT_INITIALIZED);

    MONGO_CATALOG.initialize(TEST_DATABASE_NAME, getConnectionProviderOptions());
    assertDoesNotThrow(
        () -> MONGO_CATALOG.createNamespace(TEST_IDENTIFIER.namespace(), emptyMap()));

    createCollection(TEST_IDENTIFIER);
    assertThrows(
        NamespaceAlreadyExistsException.class,
        () -> MONGO_CATALOG.createNamespace(TEST_IDENTIFIER.namespace(), emptyMap()));
  }

  @Test
  void dropNamespaceTest() {
    assertThrows(
        IllegalStateException.class,
        () -> MONGO_CATALOG.dropNamespace(TEST_IDENTIFIER.namespace()));

    MONGO_CATALOG.initialize(TEST_DATABASE_NAME, getConnectionProviderOptions());
    assertFalse(MONGO_CATALOG.dropNamespace(TEST_IDENTIFIER.namespace()));

    createCollection(TEST_IDENTIFIER);
    assertTrue(MONGO_CATALOG.dropNamespace(TEST_IDENTIFIER.namespace()));
  }

  @Test
  void alterNamespaceTest() {
    assertThrows(
        IllegalStateException.class,
        () ->
            MONGO_CATALOG.alterNamespace(
                TEST_IDENTIFIER.namespace(), NamespaceChange.setProperty("prop", "value")),
        NOT_INITIALIZED);
    MONGO_CATALOG.initialize(TEST_DATABASE_NAME, getConnectionProviderOptions());
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            MONGO_CATALOG.alterNamespace(
                TEST_IDENTIFIER.namespace(), NamespaceChange.setProperty("prop", "value")));
  }

  @Test
  void listTablesTest() {
    assertThrows(
        IllegalStateException.class,
        () -> MONGO_CATALOG.listTables(TEST_IDENTIFIER.namespace()),
        NOT_INITIALIZED);

    MONGO_CATALOG.initialize(TEST_DATABASE_NAME, getConnectionProviderOptions());
    assertEquals(0, MONGO_CATALOG.listTables(TEST_IDENTIFIER.namespace()).length);

    createCollections(
        TEST_DATABASE_NAME,
        IntStream.rangeClosed(1, 5)
            .boxed()
            .map(i -> format("TEST_%s", i))
            .collect(Collectors.toList()));

    assertEquals(5, MONGO_CATALOG.listTables(TEST_IDENTIFIER.namespace()).length);

    createView(TEST_IDENTIFIER);
    assertEquals(5, MONGO_CATALOG.listTables(TEST_IDENTIFIER.namespace()).length);
  }

  @Test
  void dropTableTest() {
    assertThrows(
        IllegalStateException.class,
        () ->
            MONGO_CATALOG.dropTable(
                Identifier.of(TEST_IDENTIFIER.namespace(), TEST_COLLECTION_NAME)),
        NOT_INITIALIZED);
    MONGO_CATALOG.initialize(TEST_DATABASE_NAME, getConnectionProviderOptions());

    assertFalse(MONGO_CATALOG.dropTable(TEST_IDENTIFIER));
    assertFalse(MONGO_CATALOG.dropTable(TEST_IDENTIFIER_ALT));
    assertFalse(
        MONGO_CATALOG.dropTable(Identifier.of(new String[] {"a", "b"}, TEST_COLLECTION_NAME)));

    createCollection(TEST_IDENTIFIER);
    createCollection(TEST_IDENTIFIER_ALT);

    assertTrue(MONGO_CATALOG.dropTable(TEST_IDENTIFIER));
    assertTrue(MONGO_CATALOG.dropTable(TEST_IDENTIFIER_ALT));

    createCollection(TEST_IDENTIFIER);
    createView(TEST_IDENTIFIER);

    assertFalse(MONGO_CATALOG.dropTable(Identifier.of(TEST_IDENTIFIER.namespace(), "VIEW")));
    assertTrue(MONGO_CATALOG.dropTable(TEST_IDENTIFIER));
  }

  @Test
  void renameTableTest() {
    assertThrows(
        IllegalStateException.class,
        () -> MONGO_CATALOG.renameTable(TEST_IDENTIFIER, TEST_IDENTIFIER_ALT),
        NOT_INITIALIZED);

    MONGO_CATALOG.initialize(TEST_DATABASE_NAME, getConnectionProviderOptions());

    assertThrows(
        NoSuchTableException.class,
        () -> MONGO_CATALOG.renameTable(TEST_IDENTIFIER, TEST_IDENTIFIER_ALT));
    createCollection(TEST_IDENTIFIER);
    assertThrows(
        TableAlreadyExistsException.class,
        () -> MONGO_CATALOG.renameTable(TEST_IDENTIFIER, TEST_IDENTIFIER));

    assertEquals(1, MONGO_CATALOG.listTables(TEST_IDENTIFIER.namespace()).length);
    assertEquals(0, MONGO_CATALOG.listTables(TEST_IDENTIFIER_ALT.namespace()).length);

    assertDoesNotThrow(() -> MONGO_CATALOG.renameTable(TEST_IDENTIFIER, TEST_IDENTIFIER_ALT));

    assertEquals(0, MONGO_CATALOG.listTables(TEST_IDENTIFIER.namespace()).length);
    assertEquals(1, MONGO_CATALOG.listTables(TEST_IDENTIFIER_ALT.namespace()).length);
  }

  @Test
  void alterTableTest() {
    assertThrows(
        IllegalStateException.class,
        () -> MONGO_CATALOG.alterTable(TEST_IDENTIFIER, TableChange.setProperty("prop", "value")),
        NOT_INITIALIZED);

    MONGO_CATALOG.initialize(TEST_DATABASE_NAME, getConnectionProviderOptions());
    assertThrows(
        NoSuchTableException.class,
        () -> MONGO_CATALOG.alterTable(TEST_IDENTIFIER, TableChange.setProperty("prop", "value")));

    createCollection(TEST_IDENTIFIER);
    assertThrows(
        IllegalArgumentException.class,
        () -> MONGO_CATALOG.alterTable(TEST_IDENTIFIER, TableChange.setProperty("prop", "value")));
  }

  @Test
  void loadTableTest() throws NoSuchTableException {
    assertThrows(
        IllegalStateException.class,
        () -> MONGO_CATALOG.loadTable(TEST_IDENTIFIER),
        NOT_INITIALIZED);

    MONGO_CATALOG.initialize(TEST_DATABASE_NAME, getConnectionProviderOptions());
    assertThrows(NoSuchTableException.class, () -> MONGO_CATALOG.loadTable(TEST_IDENTIFIER));

    createCollection(TEST_IDENTIFIER);

    Map<String, String> options =
        new HashMap<>(getConnectionProviderOptions().asCaseSensitiveMap());
    options.put(MongoConfig.READ_PREFIX + MongoConfig.DATABASE_NAME_CONFIG, TEST_DATABASE_NAME);
    options.put(MongoConfig.READ_PREFIX + MongoConfig.COLLECTION_NAME_CONFIG, TEST_COLLECTION_NAME);

    assertEquals(
        new MongoTable(MongoConfig.readConfig(options)), MONGO_CATALOG.loadTable(TEST_IDENTIFIER));
  }

  @Test
  void createTableTest() throws TableAlreadyExistsException {
    StructType schema =
        new StructType()
            .add("f1", IntegerType, true)
            .add("f2", LongType, false)
            .add("f3", BooleanType, false);

    assertThrows(
        IllegalStateException.class,
        () -> MONGO_CATALOG.loadTable(TEST_IDENTIFIER),
        NOT_INITIALIZED);

    MONGO_CATALOG.initialize(TEST_DATABASE_NAME, getConnectionProviderOptions());
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            MONGO_CATALOG.createTable(
                Identifier.of(new String[0], TEST_COLLECTION_NAME),
                schema,
                new Transform[0],
                emptyMap()));
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            MONGO_CATALOG.createTable(
                Identifier.of(new String[] {"a", "b"}, TEST_COLLECTION_NAME),
                schema,
                new Transform[0],
                emptyMap()));

    createCollection(TEST_IDENTIFIER_ALT);
    assertThrows(
        TableAlreadyExistsException.class,
        () -> MONGO_CATALOG.createTable(TEST_IDENTIFIER_ALT, schema, new Transform[0], emptyMap()));
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            MONGO_CATALOG.createTable(
                TEST_IDENTIFIER, schema, new Transform[] {new CustomTransform()}, emptyMap()));
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            MONGO_CATALOG.createTable(
                TEST_IDENTIFIER, schema, new Transform[0], getConnectionProviderOptions()));

    assertEquals(
        new MongoTable(schema, MongoConfig.writeConfig(getConnectionProviderOptions())),
        MONGO_CATALOG.createTable(TEST_IDENTIFIER, schema, new Transform[0], emptyMap()));
  }

  private void createCollection(final Identifier ident) {
    createCollections(ident.namespace()[0], singletonList(ident.name()));
  }

  private void createCollections(final String databaseName, final List<String> collectionNames) {
    MONGO_CATALOG
        .getWriteConfig()
        .doWithClient(
            c -> {
              MongoDatabase db = c.getDatabase(databaseName);
              for (String collectionName : collectionNames) {
                db.createCollection(collectionName);
              }
            });
  }

  private void createView(final Identifier identifier) {
    MONGO_CATALOG
        .getWriteConfig()
        .doWithClient(
            c ->
                c.getDatabase(identifier.namespace()[0])
                    .createView(
                        "VIEW",
                        identifier.name(),
                        singletonList(Aggregates.match(Filters.eq("a", 1)))));
  }

  private static Identifier createIdentifier(
      final String databaseName, final String collectionName) {
    return Identifier.of(new String[] {databaseName}, collectionName);
  }

  private static class CustomTransform implements Transform {

    @Override
    public String name() {
      return "custom";
    }

    @Override
    public NamedReference[] references() {
      return new NamedReference[0];
    }

    @Override
    public Expression[] arguments() {
      return new Expression[0];
    }

    @Override
    public String describe() {
      return "custom for testing";
    }
  }
}
