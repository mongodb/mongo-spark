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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.bson.BsonTimestamp;
import org.bson.Document;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static scala.collection.JavaConversions.asScalaBuffer;

/**
 * Utility class for converting Documents to Spark SQL Rows.
 */
public final class DocumentRowConverter {
    /**
     * Converts a document to a row according to the given schema.
     *
     * @param document the document
     * @param schema the schema
     * @return the row
     */
    public static Row documentToRow(final Document document, final StructType schema) {
        StructField[] schemaFields = schema.fields();
        Object[] fields = new Object[schemaFields.length];

        for (int i = 0; i < schemaFields.length; i++) {
            if (document.containsKey(schemaFields[i].name())) {
                fields[i] = toDataType(document.get(schemaFields[i].name()), schemaFields[i].dataType());
            }
        }

        return RowFactory.create(fields);
    }

    /**
     * Converts an object from a Document to the corresponding Spark SQL type.
     *
     * @param element the element
     * @param elementType the DataType of the element
     * @return the Spark SQL equivalent of the object
     */
    private static Object toDataType(final Object element, final DataType elementType) {
        // currently, will throw a ClassCastException when accesing the value through a Spark SQL query
        // if the value in the document does not correspond to its elementType
        if (elementType instanceof TimestampType) {
            return new java.sql.Timestamp(((BsonTimestamp) element).getTime() * 1000L);
        } else if (elementType instanceof DateType) {
            return new java.sql.Date(((Date) element).getTime());
        } else if (elementType instanceof ArrayType) {
            DataType innerElementType = ((ArrayType) (elementType)).elementType();
            return asScalaBuffer(((List<?>) element).stream()
                                                    .map(innerElement -> toDataType(innerElement, innerElementType))
                                                    .collect(Collectors.toList()))
                         .toList();
        } else if (elementType instanceof StructType) {
            return documentToRow((Document) element, (StructType) elementType);
        }

        return element;
    }

    private DocumentRowConverter() {
    }
}
