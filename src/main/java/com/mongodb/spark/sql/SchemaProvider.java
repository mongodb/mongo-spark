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

import com.mongodb.spark.MongoCollectionProvider;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.apache.spark.sql.types.DataTypes.NullType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static org.apache.spark.sql.types.DataTypes.createArrayType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

/**
 * Utility class to generate a Spark SQL schema for a collection from MongoDB.
 */
public final class SchemaProvider {
    /**
     * Gets a schema for the specified mongo collection. It is required that the
     * collection provides Documents.
     *
     * Utilizes the $sample aggregation operator, available in server versions 3.1.6+.
     *
     * Note that if any key in the sampled documents has multiple types for the value
     * in different documents, then the key is dropped from the schema.
     *
     * @param collectionProvider the collection provider
     * @param sampleSize a positive integer sample size to draw from the collection
     * @return the schema for the collection
     */
    public static StructType getSchema(final MongoCollectionProvider<Document> collectionProvider, final int sampleSize) {
        if (!(sampleSize > 0)) {
            throw new IllegalArgumentException("sampleSize must be greater than 0. provided sampleSize: " + sampleSize);
        }

        List<Document> documents = collectionProvider.getCollection()
                                                     .aggregate(singletonList(new Document("$sample", new Document("size", sampleSize))))
                                                     .into(new ArrayList<>());

        Map<String, DataType> keyValueDataTypeMap = new HashMap<>();
        List<String> ignoreKeys = new ArrayList<>();

        documents.forEach(document ->
                document.keySet().forEach(key -> {
                    DataType dataType = getDataType(document.get(key));

                    if (keyValueDataTypeMap.containsKey(key) && !keyValueDataTypeMap.get(key).sameType(dataType)) {
                        keyValueDataTypeMap.remove(key);
                        ignoreKeys.add(key);
                    } else if (!ignoreKeys.contains(key)) {
                        keyValueDataTypeMap.put(key, dataType);
                    }
                })
        );

        List<StructField> fields = keyValueDataTypeMap.entrySet()
                                                      .stream()
                                                      .map(entry -> createStructField(entry.getKey(), entry.getValue(), true))
                                                      .collect(Collectors.toList());
        return createStructType(fields);
    }

    /**
     * Generates a Spark SQL schema from the given document.
     * This is useful if the structure of documents in the database is known,
     * especially if the types of values for the keys are known.
     *
     * Warning: if the document provided incorrectly describes the structure
     * of documents in the collection, runtime exceptions may occur.
     *
     * @param document the document
     * @return the Spark SQL schema for the document
     */
    public static StructType getSchemaFromDocument(final Document document) {
        List<StructField> fields = document.entrySet()
                                           .stream()
                                           .map(entry -> createStructField(entry.getKey(), getDataType(entry.getValue()), true))
                                           .collect(Collectors.toList());
        return createStructType(fields);
    }

    /**
     * This is a helper function used to get the Spark SQL DataType for a given field
     * in a Document.
     *
     * Support is currently provided for the following types (from Document):
     *  IntegerType
     *  LongType
     *  FloatType
     *  DoubleType
     *  StringType
     *  BinaryType
     *  DateType *
     *  TimestampType
     *  StructType
     *  ArrayType
     *
     * At this point in time, no support is offered for ByteType, ShortType, DecimalType, MapType, StructField.
     *
     * * Note that the DateType used in Spark SQL only records year, month, and day. Precision from
     *   BsonTimestamp will be lost.
     *
     * @param object the object
     * @return the DataType of the object
     */
    private static DataType getDataType(final Object object) {
        Class objectClass = object.getClass();

        if (objectClass.equals(ObjectId.class)) {
            return StringType;
        }
        else if (objectClass.equals(Document.class)) {
            return getSchemaFromDocument((Document) object);
        }
        else if (objectClass.equals(ArrayList.class)) {
            return createArrayType(getArrayElementType((List) object));
        }
        else if (objectClass.equals(BsonTimestamp.class)) {
            return TimestampType;
        }
        // this will throw an exception if the class is not supported in Spark SQL type system
        return DataType.fromJson("\"" + objectClass.getSimpleName().toLowerCase() + "\"");
    }

    /**
     * Gets the DataType of the elements in the Array. Since arrays in documents
     * are represented by ArrayLists, the method takes a List as a parameter.
     *
     * Since ArrayTypes in Spark SQL must be homogenous, heterogenous arrays are ignored.
     *
     * @param list the list representing the array
     * @return the DataType of the elements in the array, or null if the list is empty or heterogeneous
     */
    private static DataType getArrayElementType(final List list) {
        if (list.isEmpty()) {
            return NullType;
        }

        Class elementClass = list.get(0).getClass();
        for (Object element : list) {
            if (!element.getClass().equals(elementClass)) {
                return NullType;
            }
        }

        // Deal with documents with differing structures, since they are
        // technically homogenous, but taking the schema generated by the first
        // document could potentially overlook elements in the rest of the
        // array, e.g. [{a : 1}, {b : 1}] should have a schema [a, b]
        if (elementClass.equals(Document.class)) {
            StructType schema = (StructType) getDataType(list.get(0));
            for (int i = 1; i < list.size(); i++) {
                // TODO: deal with possibly thrown SparkException
                schema = schema.merge((StructType) getDataType(list.get(i)));
            }
            return schema;
        }
        return getDataType(list.get(0));
    }

    private SchemaProvider() {
    }
}
