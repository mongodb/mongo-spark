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
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

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

        return getSchemaFromDocuments(documents);
    }

    /**
     * Gets a schema from the specified Documents. The documents should be
     * a representative sample of the collection, whether from $sample
     * or sampling the RDD directly.
     *
     * @param documents the documents
     * @return the schema for the collection
     */
    private static StructType getSchemaFromDocuments(final List<Document> documents) {
        Map<String, DataType> keyValueDataTypeMap = new HashMap<>();

        documents.forEach(document ->
            document.keySet().forEach(key -> {
                DataType dataType;
                try {
                    dataType = getDataType(document.get(key));
                } catch (SkipFieldException e) {
                    // just skip the field
                    return;
                }

                if (keyValueDataTypeMap.containsKey(key)) {
                    keyValueDataTypeMap.put(key, getMatchingDataType(keyValueDataTypeMap.get(key), dataType));
                } else {
                    keyValueDataTypeMap.put(key, dataType);
                }
            })
        );

        List<StructField> fields = keyValueDataTypeMap.entrySet()
                                                      .stream()
                                                      .map(entry -> DataTypes.createStructField(entry.getKey(), entry.getValue(), true))
                                                      .collect(Collectors.toList());

        return DataTypes.createStructType(fields);
    }

    /**
     * Gets the matching DataType for the input DataTypes.
     *
     * For simple types, returns a ConflictType if the DataTypes do not match.
     *
     * For complex types:
     * - ArrayTypes: if the DataTypes of the elements cannot be matched, then
     *               an ArrayType(ConflictType, true) is returned.
     * - StructTypes: for any field on which the DataTypes conflict, the field
     *                value is replaced with a ConflictType.
     *
     * @param thisType the DataType of the first element
     * @param thatType the DataType of the second element
     * @return the DataType that matches on the input DataTypes
     */
    private static DataType getMatchingDataType(final DataType thisType, final DataType thatType) {
        if (thisType instanceof ArrayType && thatType instanceof ArrayType) {
            ArrayType thisArrayType = (ArrayType) thisType;
            ArrayType thatArrayType = (ArrayType) thatType;

            DataType matchingElementDataType = getMatchingDataType(thisArrayType.elementType(), thatArrayType.elementType());

            return DataTypes.createArrayType(matchingElementDataType);
        }

        if (thisType instanceof StructType && thatType instanceof StructType) {
            StructType thisStructType = (StructType) thisType;
            StructType thatStructType = (StructType) thatType;

            List<String> thisTypeFieldNames = new ArrayList<>(Arrays.asList(thisStructType.fieldNames()));
            List<String> thatTypeFieldNames = new ArrayList<>(Arrays.asList(thatStructType.fieldNames()));

            List<String> intersectionFieldNames = new ArrayList<>(thisTypeFieldNames);
            intersectionFieldNames.retainAll(thatTypeFieldNames);

            StructField[] thisTypeFields = thisStructType.fields();
            StructField[] thatTypeFields = thatStructType.fields();

            List<StructField> fields = new ArrayList<>();

            intersectionFieldNames.forEach(fieldName -> {
                DataType thisFieldDataType = thisTypeFields[thisStructType.fieldIndex(fieldName)].dataType();
                DataType thatFieldDataType = thatTypeFields[thatStructType.fieldIndex(fieldName)].dataType();

                DataType matchingElementDataType = getMatchingDataType(thisFieldDataType, thatFieldDataType);

                fields.add(DataTypes.createStructField(fieldName, matchingElementDataType, true));
            });

            thisTypeFieldNames.removeAll(intersectionFieldNames);
            thatTypeFieldNames.removeAll(intersectionFieldNames);

            thisTypeFieldNames.forEach(fieldName -> fields.add(thisTypeFields[thisStructType.fieldIndex(fieldName)]));
            thatTypeFieldNames.forEach(fieldName -> fields.add(thatTypeFields[thatStructType.fieldIndex(fieldName)]));

            return DataTypes.createStructType(fields);
        }

        return thisType.sameType(thatType) ? thisType : ConflictType.CONFLICT_TYPE;
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
                                           .map(entry -> {
                                               try {
                                                   return DataTypes.createStructField(entry.getKey(), getDataType(entry.getValue()), true);
                                               } catch (SkipFieldException e) {
                                                   return null;
                                               }
                                           })
                                           .filter(field -> field != null)
                                           .collect(Collectors.toList());

        return DataTypes.createStructType(fields);
    }

    /**
     * This is a helper function used to get the Spark SQL DataType for a given field
     * in a Document.
     *
     * Support is currently provided for the following types:
     *  ByteType
     *  ShortType
     *  IntegerType
     *  LongType
     *  FloatType
     *  DoubleType
     *  StringType
     *  BinaryType
     *  DateType *
     *  TimestampType
     *  StructType
     *  ArrayType **
     *
     * At this point in time, no support is offered for DecimalType, MapType, StructField.
     *
     * * Note that the DateType used in Spark SQL only records year, month, and day. Precision from
     *   Date will be lost.
     *
     * ** Since ArrayTypes in Spark SQL must be homogenous, heterogenous arrays are marked
     *    as ArrayType(ConflictType, true).
     *
     * @param object the object
     * @return the DataType of the object
     */
    private static DataType getDataType(final Object object) throws SkipFieldException {
        if (object == null) {
            throw new SkipFieldException("Skip null value field");
        }

        if (object instanceof ObjectId) {
            return DataTypes.StringType;
        }
        else if (object instanceof Document) {
            return getSchemaFromDocument((Document) object);
        }
        else if (object instanceof List) {
            // throws SparkException
            return DataTypes.createArrayType(getArrayElementType((List) object));
        }
        else if (object instanceof BsonTimestamp) {
            return DataTypes.TimestampType;
        }

        // this will throw an exception if the class is not supported in Spark SQL type system
        try {
            return DataType.fromJson("\"" + object.getClass().getSimpleName().toLowerCase() + "\"");
        } catch (NoSuchElementException e) {
            // may need to just catch Exception
            return ConflictType.CONFLICT_TYPE;
        }
    }

    /**
     * Gets the DataType of the elements in the Array. Since arrays in documents
     * are represented by Lists, the method takes a List as a parameter.
     *
     * Since ArrayTypes in Spark SQL must be homogenous, heterogenous arrays are marked
     * as ArrayType(ConflictType, true).
     *
     * @param arrayElements the list representing the array
     * @return the DataType of the elements in the array, or null if the list is empty or heterogeneous
     */
    private static DataType getArrayElementType(final List arrayElements) throws SkipFieldException {
        if (arrayElements.isEmpty()) {
            throw new SkipFieldException("Skip empty array field");
        }

        Object firstNonNullElement = null;
        for (Object element : arrayElements) {
            if (element != null) {
                firstNonNullElement = element;
                break;
            }
        }

        if (firstNonNullElement == null) {
            throw new SkipFieldException("Skip array of null values field");
        }

        // preliminary run through - if classes don't match up, then it's not homogeneous
        Class firstNonNullElementClass = firstNonNullElement.getClass();
        for (Object element : arrayElements) {
            if (element != null && !element.getClass().equals(firstNonNullElementClass)) {
                return ConflictType.CONFLICT_TYPE;
            }
        }

        DataType elementType = getDataType(firstNonNullElement);
        if (firstNonNullElement instanceof Document || firstNonNullElement instanceof List) {
            for (Object element : arrayElements) {
                if (element != null && !element.equals(firstNonNullElement)) {
                    elementType = getMatchingDataType(elementType, getDataType(element));
                }
            }
        }

        return elementType;
    }

    private SchemaProvider() {
    }
}
