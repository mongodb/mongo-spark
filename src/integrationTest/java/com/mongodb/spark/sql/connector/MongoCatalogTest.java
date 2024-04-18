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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.exceptions.MongoSparkException;
import com.mongodb.spark.sql.connector.mongodb.MongoSparkConnectorTestCase;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.junit.platform.commons.function.Try;

public class MongoCatalogTest extends MongoSparkConnectorTestCase {
  private static final MongoCatalog MONGO_CATALOG = new MongoCatalog();
  private static final String DATABASE_NAME = "SPARK_CONNECTOR_TEST";
  private static final String DATABASE_NAME_ALT = "SPARK_CONNECTOR_TEST_ALT";
  private static final String COLLECTION_NAME = "COLL";
  private static final String COLLECTION_NAME_ALT = "COLL_ALT";

  private static final AtomicInteger ATOMIC_INTEGER = new AtomicInteger();
  private static final String NOT_INITIALIZED = "The MongoCatalog is has not been initialized";

  @AfterEach
  void afterEach() {
    MONGO_CATALOG.reset(() -> MONGO_CATALOG.getWriteConfig().doWithClient(c -> c.listDatabaseNames()
        .forEach(n -> {
          if (n.startsWith(DATABASE_NAME)) {
            c.getDatabase(n).drop();
          }
        })));
  }

  @Test
  void initializeTest() {
    assertDoesNotThrow(
        () -> MONGO_CATALOG.initialize(DATABASE_NAME, getConnectionProviderOptions()));
    assertThrows(
        IllegalStateException.class,
        () -> MONGO_CATALOG.initialize(DATABASE_NAME, getConnectionProviderOptions()),
        "The MongoCatalog has already been initialized.");
  }

  @Test
  void nameTest() {
    Identifier identifier = createIdentifier(DATABASE_NAME, COLLECTION_NAME);
    assertThrows(IllegalStateException.class, MONGO_CATALOG::name, NOT_INITIALIZED);

    MONGO_CATALOG.initialize(identifier.namespace()[0], getConnectionProviderOptions());
    assertEquals(identifier.namespace()[0], MONGO_CATALOG.name());
  }

  @Test
  void listNamespacesTest() throws NoSuchNamespaceException {
    Identifier identifier = createIdentifier(DATABASE_NAME, COLLECTION_NAME);
    Identifier identifierAltDatabase = createIdentifier(DATABASE_NAME_ALT, COLLECTION_NAME);

    assertThrows(IllegalStateException.class, MONGO_CATALOG::listNamespaces, NOT_INITIALIZED);
    assertThrows(
        IllegalStateException.class, () -> MONGO_CATALOG.listNamespaces(identifier.namespace()));

    MONGO_CATALOG.initialize(identifier.namespace()[0], getConnectionProviderOptions());
    createCollection(identifier);

    assertTrue(Arrays.stream(MONGO_CATALOG.listNamespaces())
        .anyMatch(s -> Arrays.equals(s, identifier.namespace())));

    assertThrows(
        NoSuchNamespaceException.class,
        () -> MONGO_CATALOG.listNamespaces(identifierAltDatabase.namespace()));
    assertEquals(0, MONGO_CATALOG.listNamespaces(identifier.namespace()).length);

    assertArrayEquals(MONGO_CATALOG.listNamespaces(), MONGO_CATALOG.listNamespaces(new String[0]));
  }

  @Test
  void namespaceExistsTest() {
    Identifier identifier = createIdentifier(DATABASE_NAME, COLLECTION_NAME);
    assertThrows(
        IllegalStateException.class,
        () -> MONGO_CATALOG.namespaceExists(identifier.namespace()),
        NOT_INITIALIZED);

    MONGO_CATALOG.initialize(identifier.namespace()[0], getConnectionProviderOptions());
    assertFalse(MONGO_CATALOG.namespaceExists(identifier.namespace()));

    createCollection(identifier);
    assertTrue(MONGO_CATALOG.namespaceExists(identifier.namespace()));
  }

  @Test
  void loadNamespaceMetadataTest() throws NoSuchNamespaceException {
    Identifier identifier = createIdentifier(DATABASE_NAME, COLLECTION_NAME);
    Identifier identifierAltDatabase = createIdentifier(DATABASE_NAME_ALT, COLLECTION_NAME);
    assertThrows(
        IllegalStateException.class,
        () -> MONGO_CATALOG.loadNamespaceMetadata(identifier.namespace()),
        NOT_INITIALIZED);

    MONGO_CATALOG.initialize(identifier.namespace()[0], getConnectionProviderOptions());
    assertThrows(
        NoSuchNamespaceException.class,
        () -> MONGO_CATALOG.loadNamespaceMetadata(identifierAltDatabase.namespace()));

    createCollection(identifier);
    assertEquals(emptyMap(), MONGO_CATALOG.loadNamespaceMetadata(identifier.namespace()));
  }

  @Test
  void createNamespaceTest() {
    Identifier identifier = createIdentifier(DATABASE_NAME, COLLECTION_NAME);
    assertThrows(
        IllegalStateException.class,
        () -> MONGO_CATALOG.createNamespace(identifier.namespace(), emptyMap()),
        NOT_INITIALIZED);

    MONGO_CATALOG.initialize(identifier.namespace()[0], getConnectionProviderOptions());
    assertDoesNotThrow(() -> MONGO_CATALOG.createNamespace(identifier.namespace(), emptyMap()));

    createCollection(identifier);
    assertThrows(
        NamespaceAlreadyExistsException.class,
        () -> MONGO_CATALOG.createNamespace(identifier.namespace(), emptyMap()));
  }

  @Test
  void dropNamespaceTest() {
    Identifier identifier = createIdentifier(DATABASE_NAME, COLLECTION_NAME);
    assertThrows(
        IllegalStateException.class, () -> MONGO_CATALOG.dropNamespace(identifier.namespace()));

    MONGO_CATALOG.initialize(identifier.namespace()[0], getConnectionProviderOptions());
    assertFalse(MONGO_CATALOG.dropNamespace(identifier.namespace()));

    createCollection(identifier);
    assertTrue(MONGO_CATALOG.dropNamespace(identifier.namespace()));
  }

  @Test
  void dropNamespaceTestCascade() {
    Identifier identifier = createIdentifier(DATABASE_NAME, COLLECTION_NAME);
    assertThrows(
        IllegalStateException.class,
        () -> MONGO_CATALOG.dropNamespace(identifier.namespace(), true));

    MONGO_CATALOG.initialize(identifier.namespace()[0], getConnectionProviderOptions());
    assertFalse(MONGO_CATALOG.dropNamespace(identifier.namespace(), true));

    createCollection(identifier);
    assertTrue(MONGO_CATALOG.dropNamespace(identifier.namespace(), true));
  }

  @Test
  void alterNamespaceTest() {
    Identifier identifier = createIdentifier(DATABASE_NAME, COLLECTION_NAME);
    assertThrows(
        IllegalStateException.class,
        () -> MONGO_CATALOG.alterNamespace(
            identifier.namespace(), NamespaceChange.setProperty("prop", "value")),
        NOT_INITIALIZED);
    MONGO_CATALOG.initialize(identifier.namespace()[0], getConnectionProviderOptions());
    assertThrows(
        UnsupportedOperationException.class,
        () -> MONGO_CATALOG.alterNamespace(
            identifier.namespace(), NamespaceChange.setProperty("prop", "value")));
  }

  @Test
  void tablesExists() {
    Identifier identifier = createIdentifier(DATABASE_NAME, COLLECTION_NAME);
    Identifier identifierAltCollection = createIdentifier(DATABASE_NAME, COLLECTION_NAME_ALT);
    assertThrows(
        IllegalStateException.class, () -> MONGO_CATALOG.tableExists(identifier), NOT_INITIALIZED);

    MONGO_CATALOG.initialize(identifier.namespace()[0], getConnectionProviderOptions());
    assertFalse(MONGO_CATALOG.tableExists(identifier));

    createCollection(identifier);
    assertTrue(MONGO_CATALOG.tableExists(identifier));
    assertFalse(MONGO_CATALOG.tableExists(identifierAltCollection));
  }

  @Test
  void listTablesTest() {
    Identifier identifier = createIdentifier(DATABASE_NAME, COLLECTION_NAME);
    assertThrows(
        IllegalStateException.class,
        () -> MONGO_CATALOG.listTables(identifier.namespace()),
        NOT_INITIALIZED);

    MONGO_CATALOG.initialize(identifier.namespace()[0], getConnectionProviderOptions());
    assertEquals(0, MONGO_CATALOG.listTables(identifier.namespace()).length);

    createCollections(
        identifier.namespace()[0],
        IntStream.rangeClosed(1, 5)
            .boxed()
            .map(i -> format("TEST_%s", i))
            .collect(Collectors.toList()));

    assertEquals(5, MONGO_CATALOG.listTables(identifier.namespace()).length);

    createView(identifier);
    assertEquals(5, MONGO_CATALOG.listTables(identifier.namespace()).length);
  }

  @Test
  void dropTableTest() {
    Identifier identifier = createIdentifier(DATABASE_NAME, COLLECTION_NAME);
    Identifier identifierAltDatabase = createIdentifier(DATABASE_NAME_ALT, COLLECTION_NAME);
    assertThrows(
        IllegalStateException.class,
        () -> MONGO_CATALOG.dropTable(Identifier.of(identifier.namespace(), COLLECTION_NAME)),
        NOT_INITIALIZED);
    MONGO_CATALOG.initialize(identifier.namespace()[0], getConnectionProviderOptions());

    assertFalse(MONGO_CATALOG.dropTable(identifier));
    assertFalse(MONGO_CATALOG.dropTable(identifierAltDatabase));
    assertFalse(MONGO_CATALOG.dropTable(Identifier.of(new String[] {"a", "b"}, COLLECTION_NAME)));

    createCollection(identifier);
    createCollection(identifierAltDatabase);

    assertTrue(MONGO_CATALOG.dropTable(identifier));
    assertTrue(MONGO_CATALOG.dropTable(identifierAltDatabase));

    createCollection(identifier);
    createView(identifier);

    assertFalse(MONGO_CATALOG.dropTable(Identifier.of(identifier.namespace(), "VIEW")));
    assertTrue(MONGO_CATALOG.dropTable(identifier));
  }

  @Test
  void renameTableTest() {
    Identifier identifier = createIdentifier(DATABASE_NAME, COLLECTION_NAME);
    Identifier identifierAltCollection = createIdentifier(DATABASE_NAME, COLLECTION_NAME_ALT);
    assertThrows(
        IllegalStateException.class,
        () -> MONGO_CATALOG.renameTable(identifier, identifierAltCollection),
        NOT_INITIALIZED);

    MONGO_CATALOG.initialize(identifier.namespace()[0], getConnectionProviderOptions());

    assertThrows(
        NoSuchTableException.class,
        () -> MONGO_CATALOG.renameTable(identifier, identifierAltCollection));

    createCollection(identifier);
    assertThrows(
        TableAlreadyExistsException.class, () -> MONGO_CATALOG.renameTable(identifier, identifier));

    Try.call(() -> {
          MONGO_CATALOG.renameTable(identifier, identifierAltCollection);
          return null;
        })
        .ifSuccess((x) -> assertIterableEquals(
            singletonList(identifierAltCollection),
            asList(MONGO_CATALOG.listTables(identifierAltCollection.namespace()))))
        .ifFailure(ex -> {
          // Not all server versions support renaming across databases.
          // If they don't provide a MongoSparkException
          assertInstanceOf(MongoSparkException.class, ex);
        });
  }

  @Test
  void alterTableTest() {
    Identifier identifier = createIdentifier(DATABASE_NAME, COLLECTION_NAME);
    assertThrows(
        IllegalStateException.class,
        () -> MONGO_CATALOG.alterTable(identifier, TableChange.setProperty("prop", "value")),
        NOT_INITIALIZED);

    MONGO_CATALOG.initialize(identifier.namespace()[0], getConnectionProviderOptions());
    assertThrows(
        NoSuchTableException.class,
        () -> MONGO_CATALOG.alterTable(identifier, TableChange.setProperty("prop", "value")));

    createCollection(identifier);
    assertThrows(
        IllegalArgumentException.class,
        () -> MONGO_CATALOG.alterTable(identifier, TableChange.setProperty("prop", "value")));
  }

  @Test
  void loadTableTest() throws NoSuchTableException {
    Identifier identifier = createIdentifier(DATABASE_NAME, COLLECTION_NAME);
    assertThrows(
        IllegalStateException.class, () -> MONGO_CATALOG.loadTable(identifier), NOT_INITIALIZED);

    MONGO_CATALOG.initialize(identifier.namespace()[0], getConnectionProviderOptions());
    assertThrows(NoSuchTableException.class, () -> MONGO_CATALOG.loadTable(identifier));

    createCollection(identifier);

    Map<String, String> options =
        new HashMap<>(getConnectionProviderOptions().asCaseSensitiveMap());
    options.put(
        MongoConfig.READ_PREFIX + MongoConfig.DATABASE_NAME_CONFIG, identifier.namespace()[0]);
    options.put(MongoConfig.READ_PREFIX + MongoConfig.COLLECTION_NAME_CONFIG, identifier.name());

    assertEquals(
        new MongoTable(MongoConfig.readConfig(options)), MONGO_CATALOG.loadTable(identifier));
  }

  @Test
  void createTableTest() throws TableAlreadyExistsException {
    Identifier identifier = createIdentifier(DATABASE_NAME, COLLECTION_NAME);
    Identifier identifierAltDatabase = createIdentifier(DATABASE_NAME_ALT, COLLECTION_NAME);
    StructType schema = new StructType()
        .add("f1", IntegerType, true)
        .add("f2", LongType, false)
        .add("f3", BooleanType, false);

    assertThrows(
        IllegalStateException.class, () -> MONGO_CATALOG.loadTable(identifier), NOT_INITIALIZED);

    MONGO_CATALOG.initialize(identifier.namespace()[0], getConnectionProviderOptions());
    assertThrows(
        UnsupportedOperationException.class,
        () -> MONGO_CATALOG.createTable(
            Identifier.of(new String[0], COLLECTION_NAME), schema, new Transform[0], emptyMap()));
    assertThrows(
        UnsupportedOperationException.class,
        () -> MONGO_CATALOG.createTable(
            Identifier.of(new String[] {"a", "b"}, COLLECTION_NAME),
            schema,
            new Transform[0],
            emptyMap()));

    createCollection(identifierAltDatabase);
    assertThrows(
        TableAlreadyExistsException.class,
        () ->
            MONGO_CATALOG.createTable(identifierAltDatabase, schema, new Transform[0], emptyMap()));
    assertThrows(
        UnsupportedOperationException.class,
        () -> MONGO_CATALOG.createTable(
            identifier, schema, new Transform[] {new CustomTransform()}, emptyMap()));
    assertThrows(
        UnsupportedOperationException.class,
        () -> MONGO_CATALOG.createTable(
            identifier, schema, new Transform[0], getConnectionProviderOptions()));

    assertEquals(
        new MongoTable(schema, MongoConfig.writeConfig(getConnectionProviderOptions())),
        MONGO_CATALOG.createTable(identifier, schema, new Transform[0], emptyMap()));
  }

  private void createCollection(final Identifier ident) {
    createCollections(ident.namespace()[0], singletonList(ident.name()));
  }

  private void createCollections(final String databaseName, final List<String> collectionNames) {
    MONGO_CATALOG.getWriteConfig().doWithClient(c -> {
      MongoDatabase db = c.getDatabase(databaseName);
      for (String collectionName : collectionNames) {
        db.createCollection(collectionName);
      }
    });
  }

  private void createView(final Identifier identifier) {
    MONGO_CATALOG.getWriteConfig().doWithClient(c -> c.getDatabase(identifier.namespace()[0])
        .createView(
            "VIEW", identifier.name(), singletonList(Aggregates.match(Filters.eq("a", 1)))));
  }

  private static Identifier createIdentifier(
      final String databaseName, final String collectionName) {
    String suffix = "_" + ATOMIC_INTEGER.incrementAndGet();
    return Identifier.of(new String[] {databaseName + suffix}, collectionName + suffix);
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
