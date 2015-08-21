/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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
 */

package com.mongodb.spark.sql;

import com.mongodb.spark.MongoClientProvider;
import com.mongodb.spark.MongoCollectionProvider;
import com.mongodb.spark.MongoSparkClientProvider;
import com.mongodb.spark.MongoSparkCollectionProvider;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static com.mongodb.spark.sql.DocumentRowConverter.documentToRow;
import static org.apache.spark.sql.types.DataTypes.createArrayType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static scala.collection.JavaConversions.asScalaBuffer;

public class MongoSparkSQLTest {
    // this field is implicitly added when inserting into the database
    private StructField underscoreIdStringTypeField = createStructField("_id", DataTypes.StringType, true);

    @Test
    public void singleDoubleTypeTest() {
        Document document = new Document("a", 1.0);

        StructType schema = SchemaProvider.getSchemaFromDocument(document);

        StructField aDoubleField = createStructField("a", DataTypes.DoubleType, true);
        StructType expectedSchema = createStructType(Collections.singletonList(aDoubleField));

        assertTrue(schema.sameType(expectedSchema));

        Row row = documentToRow(document, schema);
        Row expectedRow = RowFactory.create(1.0);

        assertEquals(expectedRow, row);
    }

    @Test
    public void singleStringTypeTest() {
        Document document = new Document("a", "b");

        StructType schema = SchemaProvider.getSchemaFromDocument(document);

        StructField aStringField = createStructField("a", DataTypes.StringType, true);
        StructType expectedSchema = createStructType(Collections.singletonList(aStringField));

        assertTrue(schema.sameType(expectedSchema));

        Row row = documentToRow(document, schema);
        Row expectedRow = RowFactory.create("b");

        assertEquals(expectedRow, row);
    }

    @Test
    public void singleBinaryTypeTest() {
        Document document = new Document("a", new Binary(new byte[]{1}));

        StructType schema = SchemaProvider.getSchemaFromDocument(document);

        StructField aBinaryField = createStructField("a", DataTypes.BinaryType, true);
        StructType expectedSchema = createStructType(Collections.singletonList(aBinaryField));

        assertTrue(schema.sameType(expectedSchema));

        Row row = documentToRow(document, schema);
        Row expectedRow = RowFactory.create(new Binary(new byte[]{1}));

        assertEquals(expectedRow, row);
    }

    @Test
    public void singleTimestampTypeTest() {
        // truncate the nanoseconds value, since this precision is lost when creating the BsonTimestamp
        long truncatedTimeMillis = (System.currentTimeMillis() / 1000L) * 1000L;
        Document document = new Document("a", new BsonTimestamp(((int) (truncatedTimeMillis / 1000L)), 0));

        StructType schema = SchemaProvider.getSchemaFromDocument(document);

        StructField aTimestampField = createStructField("a", DataTypes.TimestampType, true);
        StructType expectedSchema = createStructType(Collections.singletonList(aTimestampField));

        assertTrue(schema.sameType(expectedSchema));

        Row row = documentToRow(document, schema);

        Row expectedRow = RowFactory.create(new java.sql.Timestamp(truncatedTimeMillis));

        assertEquals(expectedRow, row);
    }

    @Test
    public void singleDateTypeTest() {
        long startTimeMillis = System.currentTimeMillis();
        Document document = new Document("a", new Date(startTimeMillis));

        StructType schema = SchemaProvider.getSchemaFromDocument(document);

        StructField aDateField = createStructField("a", DataTypes.DateType, true);
        StructType expectedSchema = createStructType(Collections.singletonList(aDateField));

        assertTrue(schema.sameType(expectedSchema));

        Row row = documentToRow(document, schema);
        Row expectedRow = RowFactory.create(new java.sql.Date(startTimeMillis));

        assertEquals(expectedRow, row);
    }

    @Test
    public void singleArrayWithSimpleElementTypeTest() {
        Document document = new Document("a", Collections.singletonList(1.0));

        StructType schema = SchemaProvider.getSchemaFromDocument(document);

        StructField aArrayOfDoubleField = createStructField("a", createArrayType(DataTypes.DoubleType), true);
        StructType expectedSchema = createStructType(Collections.singletonList(aArrayOfDoubleField));

        assertTrue(schema.sameType(expectedSchema));

        Row row = documentToRow(document, schema);
        Row expectedRow = RowFactory.create(asScalaBuffer(Collections.singletonList(1.0)).toList());

        assertEquals(expectedRow, row);
    }

    @Test
    public void singleStructTypeTest() {
        Document document = new Document("a", new Document("b", 1.0));

        StructType schema = SchemaProvider.getSchemaFromDocument(document);

        StructField bDoubleField = createStructField("b", DataTypes.DoubleType, true);
        StructType subDocumentStructType = createStructType(Collections.singletonList(bDoubleField));
        StructField aStructTypeField = createStructField("a", subDocumentStructType, true);
        StructType expectedSchema = createStructType(Collections.singletonList(aStructTypeField));

        assertTrue(schema.sameType(expectedSchema));

        Row row = documentToRow(document, schema);
        Row expectedRow = RowFactory.create(RowFactory.create(1.0));

        assertEquals(expectedRow, row);
    }

    @Test
    public void shouldGenerateSameSchema() {
        // { a : 1.0, b : { a : 1.0 }, c : { a : [ 1.0 ] }, d : "string", e : true, f : ISODate(...), g : Timestamp(...) }
        // { a : 2.0, b : { a : 2.0 }, c : { a : [ 2.0 ] }, d : "gnirts", e : true, f : ISODate(...), g : Timestamp(...) }
        Document document1 = new Document("a", 1.0)
                                  .append("b", new Document("a", 1.0))
                                  .append("c", new Document("a", Collections.singletonList(1.0)))
                                  .append("d", "string")
                                  .append("e", true)
                                  .append("f", new Date())
                                  .append("g", new BsonTimestamp());

        Document document2 = new Document("a", 2.0)
                                  .append("b", new Document("a", 2.0))
                                  .append("c", new Document("a", Collections.singletonList(2.0)))
                                  .append("d", "gnirts")
                                  .append("e", false)
                                  .append("f", new Date())
                                  .append("g", new BsonTimestamp());

        StructType schema1 = SchemaProvider.getSchemaFromDocument(document1);
        StructType schema2 = SchemaProvider.getSchemaFromDocument(document2);

        assertTrue(schema1.sameType(schema2));

        StructField aDoubleField = createStructField("a", DataTypes.DoubleType, true);
        StructField aArrayTypeOfDoubles = createStructField("a", createArrayType(DataTypes.DoubleType), true);
        StructField bStructTypeField =
                createStructField("b", createStructType(Collections.singletonList(aDoubleField)), true);
        StructField cStructTypeWithArrayOfInteger =
                createStructField("c", createStructType(Collections.singletonList(aArrayTypeOfDoubles)), true);
        StructField dStringField = createStructField("d", DataTypes.StringType, true);
        StructField eBooleanField = createStructField("e", DataTypes.BooleanType, true);
        StructField fDateField = createStructField("f", DataTypes.DateType, true);
        StructField gTimestampField = createStructField("g", DataTypes.TimestampType, true);

        StructType expectedSchema = createStructType(Arrays.asList(aDoubleField, bStructTypeField, cStructTypeWithArrayOfInteger,
                dStringField, eBooleanField, fDateField, gTimestampField));

        assertTrue(expectedSchema.sameType(schema1));
        assertTrue(expectedSchema.sameType(schema2));
    }

    @Test
    public void shouldGenerateSameSchemaEmbeddedDocuments() {
        // { a : { b: { c : { d : 1.0 } } } }
        // { a : { b: { c : { d : 2.0 } } } }
        Document document1 = new Document("a", new Document("b", new Document("c", new Document("d", 1.0))));
        Document document2 = new Document("a", new Document("b", new Document("c", new Document("d", 2.0))));

        StructType schema1 = SchemaProvider.getSchemaFromDocument(document1);
        StructType schema2 = SchemaProvider.getSchemaFromDocument(document2);

        assertTrue(schema1.sameType(schema2));

        StructField dDoubleTypeField = createStructField("d", DataTypes.DoubleType, true);
        StructType cStructType = createStructType(Collections.singletonList(dDoubleTypeField));
        StructField cStructTypeField = createStructField("c", cStructType, true);
        StructType bStructType = createStructType(Collections.singletonList(cStructTypeField));
        StructField bStructTypeField = createStructField("b", bStructType, true);
        StructType aStructType = createStructType(Collections.singletonList(bStructTypeField));
        StructField aStructTypeField = createStructField("a", aStructType, true);

        StructType expectedSchema = createStructType(Collections.singletonList(aStructTypeField));

        assertTrue(expectedSchema.sameType(schema1));
        assertTrue(expectedSchema.sameType(schema2));
    }

    @Test
    public void shouldGenerateConflictTypes() {
        // { a: { b: { c: { d: 1.0 } } }, b: 1.0, c: true, d: [ 1.0 ] }
        // { a: { b: { c: { d: "1.0" } } }, b: "1.0", c: "true", d: [ "1.0" ] }
        Document document1 = new Document("a", new Document("b", new Document("c", new Document("d", 1.0))))
                                  .append("b", 1.0)
                                  .append("c", true)
                                  .append("d", Collections.singletonList(1.0));
        Document document2 = new Document("a", new Document("b", new Document("c", new Document("d", "1.0"))))
                                  .append("b", "1.0")
                                  .append("c", "true")
                                  .append("d", Collections.singletonList("1.0"));

        List<Document> documents = Arrays.asList(document1, document2);

        // insert to a collection
        MongoClientProvider clientProvider = new MongoSparkClientProvider("mongodb://localhost:27017/?maxIdleTimeMS=250");
        MongoCollectionProvider<Document> collectionProvider =
                new MongoSparkCollectionProvider<>(Document.class, clientProvider, "spark_test", "sql_test");

        collectionProvider.getCollection().drop();
        collectionProvider.getCollection().insertMany(documents);

        StructType schema = SchemaProvider.getSchema(collectionProvider, documents.size());

        StructField dConflictTypeField = createStructField("d", ConflictType.CONFLICT_TYPE, true);
        StructType cStructType = createStructType(Collections.singletonList(dConflictTypeField));
        StructField cStructTypeField = createStructField("c", cStructType, true);
        StructType bStructType = createStructType(Collections.singletonList(cStructTypeField));
        StructField bStructTypeField = createStructField("b", bStructType, true);
        StructType aStructType = createStructType(Collections.singletonList(bStructTypeField));
        StructField aStructTypeField = createStructField("a", aStructType, true);

        StructField bConflictTypeField = createStructField("b", ConflictType.CONFLICT_TYPE, true);

        StructField cConflictTypeField = createStructField("c", ConflictType.CONFLICT_TYPE, true);

        StructField dArrayConflictTypeField = createStructField("d", createArrayType(ConflictType.CONFLICT_TYPE), true);

        StructType expectedSchema = createStructType(Arrays.asList(aStructTypeField, bConflictTypeField, cConflictTypeField,
                dArrayConflictTypeField, underscoreIdStringTypeField));

        assertTrue(expectedSchema.sameType(schema));
    }

    @Test
    public void shouldMergeStructTypesWithConflicts() {
        // { a: { b: { c: { d: 1.0 } } }, b: 1.0, c: true }
        // { a: { b: { c: { d: "1.0" } } }, b: 2.0, c: "true" }
        Document document1 = new Document("a", new Document("b", new Document("c", new Document("d", 1.0))))
                                  .append("b", 1.0)
                                  .append("c", true);
        Document document2 = new Document("a", new Document("b", new Document("c", new Document("d", "1.0"))))
                                  .append("b", 2.0)
                                  .append("c", "true");

        List<Document> documents = Arrays.asList(document1, document2);

        // insert to a collection
        MongoClientProvider clientProvider = new MongoSparkClientProvider("mongodb://localhost:27017/?maxIdleTimeMS=250");
        MongoCollectionProvider<Document> collectionProvider =
                new MongoSparkCollectionProvider<>(Document.class, clientProvider, "spark_test", "sql_test");

        collectionProvider.getCollection().drop();
        collectionProvider.getCollection().insertMany(documents);

        StructType schema = SchemaProvider.getSchema(collectionProvider, documents.size());

        StructField dConflictTypeField = createStructField("d", ConflictType.CONFLICT_TYPE, true);
        StructType cStructType = createStructType(Collections.singletonList(dConflictTypeField));
        StructField cStructTypeField = createStructField("c", cStructType, true);
        StructType bStructType = createStructType(Collections.singletonList(cStructTypeField));
        StructField bStructTypeField = createStructField("b", bStructType, true);
        StructType aStructType = createStructType(Collections.singletonList(bStructTypeField));
        StructField aStructTypeField = createStructField("a", aStructType, true);

        StructField bDoubleTypeField = createStructField("b", DataTypes.DoubleType, true);

        StructField cConflictTypeField = createStructField("c", ConflictType.CONFLICT_TYPE, true);

        StructType expectedSchema =
                createStructType(Arrays.asList(aStructTypeField, bDoubleTypeField, cConflictTypeField, underscoreIdStringTypeField));

        assertTrue(expectedSchema.sameType(schema));
    }

    @Test
    public void shouldMergeStructTypesBetweenArrayTypes() {
        // { a : [ { b: 1.0 } ] }
        // { a : [ { c: 1.0 } ] }
        Document document1 = new Document("a", Collections.singletonList(new Document("b", 1.0)));
        Document document2 = new Document("a", Collections.singletonList(new Document("c", 1.0)));

        List<Document> documents = Arrays.asList(document1, document2);

        // insert to a collection
        MongoClientProvider clientProvider = new MongoSparkClientProvider("mongodb://localhost:27017/?maxIdleTimeMS=250");
        MongoCollectionProvider<Document> collectionProvider =
                new MongoSparkCollectionProvider<>(Document.class, clientProvider, "spark_test", "sql_test");

        collectionProvider.getCollection().drop();
        collectionProvider.getCollection().insertMany(documents);

        StructType schema = SchemaProvider.getSchema(collectionProvider, documents.size());

        StructField bDoubleTypeField = createStructField("b", DataTypes.DoubleType, true);
        StructField cDoubleTypeField = createStructField("c", DataTypes.DoubleType, true);

        StructType bcStructType = createStructType(Arrays.asList(bDoubleTypeField, cDoubleTypeField));
        ArrayType aArrayTypeOfBCStructType = createArrayType(bcStructType);
        StructField aArrayTypeOfBCStructTypeField = createStructField("a", aArrayTypeOfBCStructType, true);

        StructType expectedBCSchema = createStructType(Arrays.asList(aArrayTypeOfBCStructTypeField, underscoreIdStringTypeField));

        StructType cbStructType = createStructType(Arrays.asList(cDoubleTypeField, bDoubleTypeField));
        ArrayType aArrayTypeOfCBStructType = createArrayType(cbStructType);
        StructField aArrayTypeOfCBStructTypeField = createStructField("a", aArrayTypeOfCBStructType, true);

        StructType expectedCBSchema = createStructType(Arrays.asList(aArrayTypeOfCBStructTypeField, underscoreIdStringTypeField));

        // because of $sample in SchemaProvider.getSchema, the schema built may have the order
        // of the StructType fields in the elementType of the ArrayType be c, b instead of b, c
        assertTrue(expectedBCSchema.sameType(schema) || expectedCBSchema.sameType(schema));
    }

    @Test
    public void shouldMergeStructTypesWithConflictsBetweenArrayTypes() {
        // { a : [ { b: 1.0 } ] }
        // { a : [ { b: "string" } ] }
        Document document1 = new Document("a", Collections.singletonList(new Document("b", 1.0)));
        Document document2 = new Document("a", Collections.singletonList(new Document("b", "string")));

        List<Document> documents = Arrays.asList(document1, document2);

        // insert to a collection
        MongoClientProvider clientProvider = new MongoSparkClientProvider("mongodb://localhost:27017/?maxIdleTimeMS=250");
        MongoCollectionProvider<Document> collectionProvider =
                new MongoSparkCollectionProvider<>(Document.class, clientProvider, "spark_test", "sql_test");

        collectionProvider.getCollection().drop();
        collectionProvider.getCollection().insertMany(documents);

        StructType schema = SchemaProvider.getSchema(collectionProvider, documents.size());

        StructField bDoubleTypeField = createStructField("b", ConflictType.CONFLICT_TYPE, true);
        StructType bStructType = createStructType(Collections.singletonList(bDoubleTypeField));
        ArrayType aArrayTypeOfBStructType = createArrayType(bStructType);
        StructField aArrayTypeOfBCStructTypeField = createStructField("a", aArrayTypeOfBStructType, true);

        StructType expectedSchema = createStructType(Arrays.asList(aArrayTypeOfBCStructTypeField, underscoreIdStringTypeField));

        assertTrue(expectedSchema.sameType(schema));
    }

    @Test
    public void shouldSkipEmptyArrayType() {
        // { a : [ ] }
        // { a : [ 1.0 ] }
        Document document1 = new Document("a", Collections.emptyList());
        Document document2 = new Document("a", Collections.singletonList(1.0));

        List<Document> documents = Arrays.asList(document1, document2);

        // insert to a collection
        MongoClientProvider clientProvider = new MongoSparkClientProvider("mongodb://localhost:27017/?maxIdleTimeMS=250");
        MongoCollectionProvider<Document> collectionProvider =
                new MongoSparkCollectionProvider<>(Document.class, clientProvider, "spark_test", "sql_test");

        collectionProvider.getCollection().drop();
        collectionProvider.getCollection().insertMany(documents);

        StructType schema = SchemaProvider.getSchema(collectionProvider, documents.size());

        StructField aArrayOfDoubleType = createStructField("a", createArrayType(DataTypes.DoubleType), true);

        StructType expectedSchema = createStructType(Arrays.asList(aArrayOfDoubleType, underscoreIdStringTypeField));

        assertTrue(expectedSchema.sameType(schema));
    }

    @Test
    public void shouldMergeStructTypesInArray() {
        // { a : [ { b : 1.0 }, { c : 1.0 } ] }
        Document document = new Document("a", Arrays.asList(new Document("b", 1.0), new Document("c", 1.0)));

        StructType schema = SchemaProvider.getSchemaFromDocument(document);

        StructField bDoubleTypeField = createStructField("b", DataTypes.DoubleType, true);
        StructField cDoubleTypeField = createStructField("c", DataTypes.DoubleType, true);
        StructType bcStructType = createStructType(Arrays.asList(bDoubleTypeField, cDoubleTypeField));
        ArrayType aArrayTypeOfBCStructType = createArrayType(bcStructType);
        StructField aArrayTypeOfBCStructTypeField = createStructField("a", aArrayTypeOfBCStructType, true);

        StructType expectedSchema = createStructType(Collections.singletonList(aArrayTypeOfBCStructTypeField));

        assertTrue(expectedSchema.sameType(schema));
    }

    @Test
    public void shouldMergeArrayTypesInArray() {
        // { a : [ [ { b : 1.0 } ], [ { c : 1.0 } ] ] }
        Document document = new Document("a", Arrays.asList(Collections.singletonList(new Document("b", 1.0)),
                                                            Collections.singletonList(new Document("c", 1.0))));

        StructType schema = SchemaProvider.getSchemaFromDocument(document);

        StructField bDoubleTypeField = createStructField("b", DataTypes.DoubleType, true);
        StructField cDoubleTypeField = createStructField("c", DataTypes.DoubleType, true);
        StructType bcStructType = createStructType(Arrays.asList(bDoubleTypeField, cDoubleTypeField));
        ArrayType arrayTypeOfBCStructType = createArrayType(bcStructType);
        ArrayType arrayTypeOfArrayTypeOfBCStructType = createArrayType(arrayTypeOfBCStructType);
        StructField arrayTypeOfArrayTypeOfBCStructTypeField = createStructField("a", arrayTypeOfArrayTypeOfBCStructType, true);

        StructType expectedSchema = createStructType(Collections.singletonList(arrayTypeOfArrayTypeOfBCStructTypeField));

        assertTrue(expectedSchema.sameType(schema));
    }

    @Test
    public void shouldConflictTypesInArray() {
        // { a : [ [ 1.0 ], [ "string" ] ] }
        Document document = new Document("a", Arrays.asList(Collections.singletonList(1.0), Collections.singletonList("string")));

        StructType schema = SchemaProvider.getSchemaFromDocument(document);

        ArrayType arrayOfConflictType = createArrayType(ConflictType.CONFLICT_TYPE);
        StructField aArrayOfArrayOfConflictTypeField = createStructField("a", createArrayType(arrayOfConflictType), true);

        StructType expectedSchema = createStructType(Collections.singletonList(aArrayOfArrayOfConflictTypeField));

        assertTrue(expectedSchema.sameType(schema));
    }

    @Test
    public void nullTypes() {
        Document document = new Document("a", null)
                                 .append("b", Collections.singletonList(null))
                                 .append("c", new Document("d", null));

        StructType schema = SchemaProvider.getSchemaFromDocument(document);

        StructType expectedSchema =
                createStructType(Collections.singletonList(createStructField("c", createStructType(Collections.emptyList()), true)));

        assertEquals(expectedSchema, schema);
    }
}
