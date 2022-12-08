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

import static com.mongodb.spark.sql.connector.schema.ConverterHelper.BSON_VALUE_CODEC;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;

import org.bson.BsonArray;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonNumber;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.Decimal128;

import com.mongodb.spark.sql.connector.exceptions.DataException;
import com.mongodb.spark.sql.connector.interop.JavaScala;

/**
 * The helper for conversion of BsonDocuments to GenericRowWithSchema instances.
 *
 * <p>All Bson types are considered convertible to Spark types. For any Bson types that doesn't have
 * a direct conversion to a Spark type then a String type will be used.
 *
 * <p>When a {@link DataType} is provided by Spark, the conversion process will try to convert the
 * Bson type into the declared {@code DataType}.
 */
@NotNull
public final class BsonDocumentToRowConverter implements Serializable {
  private static final long serialVersionUID = 1L;
  private final Function<Row, InternalRow> rowToInternalRowFunction;
  private final StructType schema;

  /**
   * Create a new instance with the default json writer settings
   *
   * @param schema the schema for the row
   */
  public BsonDocumentToRowConverter(final StructType schema) {
    this.schema = schema;
    this.rowToInternalRowFunction = new RowToInternalRowFunction(schema);
  }

  /** @return the schema for the converter */
  public StructType getSchema() {
    return schema;
  }

  /**
   * Converts a {@code BsonDocument} into a {@link GenericRowWithSchema} using the supplied {@code
   * StructType}
   *
   * @param bsonDocument the bson document to shape into a GenericRowWithSchema.
   * @return the converted data as a Row
   */
  @VisibleForTesting
  GenericRowWithSchema toRow(final BsonDocument bsonDocument) {
    return convertToRow("", schema, bsonDocument);
  }

  /**
   * Converts a {@code BsonDocument} into a {@link InternalRow}. Uses the {@linkplain
   * BsonValue#getBsonType BSON types} of values in the {@code bsonDocument} to determine the {@code
   * StructType}
   *
   * @param bsonDocument the bson document to shape into an InternalRow.
   * @return the converted data as a Row
   */
  public InternalRow toInternalRow(final BsonDocument bsonDocument) {
    return rowToInternalRowFunction.apply(toRow(bsonDocument));
  }

  @VisibleForTesting
  Object convertBsonValue(
      final String fieldName, final DataType dataType, final BsonValue bsonValue) {
    if (bsonValue.isNull()) {
      return null;
    } else if (dataType instanceof StructType) {
      return convertToRow(fieldName, (StructType) dataType, bsonValue);
    } else if (dataType instanceof MapType) {
      return convertToMap(fieldName, (MapType) dataType, bsonValue);
    } else if (dataType instanceof ArrayType) {
      return convertToArray(fieldName, (ArrayType) dataType, bsonValue);
    } else if (dataType instanceof BinaryType) {
      return convertToBinary(fieldName, dataType, bsonValue);
    } else if (dataType instanceof BooleanType) {
      return convertToBoolean(fieldName, dataType, bsonValue);
    } else if (dataType instanceof DateType) {
      return convertToTimestamp(fieldName, dataType, bsonValue);
    } else if (dataType instanceof TimestampType) {
      return convertToTimestamp(fieldName, dataType, bsonValue);
    } else if (dataType instanceof FloatType) {
      return convertToFloat(fieldName, dataType, bsonValue);
    } else if (dataType instanceof IntegerType) {
      return convertToInteger(fieldName, dataType, bsonValue);
    } else if (dataType instanceof DoubleType) {
      return convertToDouble(fieldName, dataType, bsonValue);
    } else if (dataType instanceof ShortType) {
      return convertToShort(fieldName, dataType, bsonValue);
    } else if (dataType instanceof ByteType) {
      return convertToByte(fieldName, dataType, bsonValue);
    } else if (dataType instanceof LongType) {
      return convertToLong(fieldName, dataType, bsonValue);
    } else if (dataType instanceof DecimalType) {
      return convertToDecimal(fieldName, dataType, bsonValue);
    } else if (dataType instanceof StringType) {
      return convertToString(bsonValue);
    } else if (dataType instanceof NullType) {
      return null;
    }

    throw invalidFieldData(fieldName, dataType, bsonValue);
  }

  private GenericRowWithSchema convertToRow(
      final String fieldName, final StructType dataType, final BsonValue bsonValue) {
    if (!bsonValue.isDocument()) {
      throw invalidFieldData(fieldName, dataType, bsonValue);
    }
    BsonDocument bsonDocument = bsonValue.asDocument();
    List<Object> values = new ArrayList<>();
    for (StructField field : dataType.fields()) {
      boolean hasField = bsonDocument.containsKey(field.name());
      String fullFieldPath = createFieldPath(fieldName, field.name());
      if (hasField || field.nullable()) {
        values.add(
            hasField
                ? convertBsonValue(fullFieldPath, field.dataType(), bsonDocument.get(field.name()))
                : null);
      } else {
        throw missingFieldException(fullFieldPath, bsonDocument);
      }
    }
    return new GenericRowWithSchema(values.toArray(), dataType);
  }

  private scala.collection.Map<String, ?> convertToMap(
      final String fieldName, final MapType dataType, final BsonValue bsonValue) {
    if (!bsonValue.isDocument()) {
      throw invalidFieldData(fieldName, dataType, bsonValue);
    }
    if (!(dataType.keyType() instanceof StringType)) {
      throw invalidFieldData(fieldName, dataType, bsonValue, " Map keys must be strings.");
    }

    Map<String, Object> map = new HashMap<>();
    bsonValue
        .asDocument()
        .forEach(
            (k, v) ->
                map.put(
                    k, convertBsonValue(createFieldPath(fieldName, k), dataType.valueType(), v)));

    return JavaScala.asScala(map);
  }

  private Object[] convertToArray(
      final String fieldName, final ArrayType dataType, final BsonValue bsonValue) {
    if (!bsonValue.isArray()) {
      throw invalidFieldData(fieldName, dataType, bsonValue);
    }
    BsonArray bsonArray = bsonValue.asArray();
    ArrayList<Object> arrayList = new ArrayList<>(bsonArray.size());
    for (int i = 0; i < bsonArray.size(); i++) {
      arrayList.add(
          convertBsonValue(
              createFieldPath(fieldName, String.valueOf(i)),
              dataType.elementType(),
              bsonArray.get(i)));
    }
    return arrayList.toArray();
  }

  private byte[] convertToBinary(
      final String fieldName, final DataType dataType, final BsonValue bsonValue) {
    switch (bsonValue.getBsonType()) {
      case STRING:
        return bsonValue.asString().getValue().trim().getBytes(StandardCharsets.UTF_8);
      case BINARY:
        return bsonValue.asBinary().getData();
      case DOCUMENT:
        return documentToByteArray(bsonValue.asDocument());
      default:
        throw invalidFieldData(fieldName, dataType, bsonValue);
    }
  }

  private boolean convertToBoolean(
      final String fieldName, final DataType dataType, final BsonValue bsonValue) {
    if (!bsonValue.isBoolean()) {
      throw invalidFieldData(fieldName, dataType, bsonValue);
    }
    return bsonValue.asBoolean().getValue();
  }

  private Timestamp convertToTimestamp(
      final String fieldName, final DataType dataType, final BsonValue bsonValue) {
    return new Timestamp(convertToLong(fieldName, dataType, bsonValue));
  }

  private float convertToFloat(
      final String fieldName, final DataType dataType, final BsonValue bsonValue) {
    return (float) convertToBsonNumber(fieldName, dataType, bsonValue).doubleValue();
  }

  private double convertToDouble(
      final String fieldName, final DataType dataType, final BsonValue bsonValue) {
    return convertToBsonNumber(fieldName, dataType, bsonValue).doubleValue();
  }

  private int convertToInteger(
      final String fieldName, final DataType dataType, final BsonValue bsonValue) {
    return convertToBsonNumber(fieldName, dataType, bsonValue).intValue();
  }

  private short convertToShort(
      final String fieldName, final DataType dataType, final BsonValue bsonValue) {
    return (short) convertToBsonNumber(fieldName, dataType, bsonValue).intValue();
  }

  private long convertToLong(
      final String fieldName, final DataType dataType, final BsonValue bsonValue) {
    return convertToBsonNumber(fieldName, dataType, bsonValue).longValue();
  }

  private BigDecimal convertToDecimal(
      final String fieldName, final DataType dataType, final BsonValue bsonValue) {
    BsonNumber bsonNumber = convertToBsonNumber(fieldName, dataType, bsonValue);
    switch (bsonNumber.getBsonType()) {
      case DOUBLE:
        return BigDecimal.valueOf(bsonNumber.doubleValue());
      case INT32:
      case INT64:
        return BigDecimal.valueOf(bsonNumber.longValue());
      default:
        return bsonNumber.decimal128Value().bigDecimalValue();
    }
  }

  private byte convertToByte(
      final String fieldName, final DataType dataType, final BsonValue bsonValue) {
    return (byte) convertToBsonNumber(fieldName, dataType, bsonValue).longValue();
  }

  private String convertToString(final BsonValue bsonValue) {
    switch (bsonValue.getBsonType()) {
      case STRING:
        return bsonValue.asString().getValue();
      case SYMBOL:
        return bsonValue.asSymbol().getSymbol();
      case DOCUMENT:
        return bsonValue.asDocument().toJson(ConverterHelper.DEFAULT_JSON_WRITER_SETTINGS);
      default:
        String value =
            new BsonDocument("v", bsonValue).toJson(ConverterHelper.DEFAULT_JSON_WRITER_SETTINGS);
        // Strip down to just the value
        value = value.substring(6, value.length() - 1);
        // Remove unnecessary quotes of BsonValues converted to Strings.
        // Such as BsonBinary base64 string representations
        if (value.startsWith("\"") && value.endsWith("\"")) {
          value = value.substring(1, value.length() - 1);
        }
        return value;
    }
  }

  private BsonNumber convertToBsonNumber(
      final String fieldName, final DataType dataType, final BsonValue bsonValue) {
    switch (bsonValue.getBsonType()) {
      case DATE_TIME:
        return new BsonInt64(bsonValue.asDateTime().getValue());
      case TIMESTAMP:
        return new BsonInt64(SECONDS.toMillis(bsonValue.asTimestamp().getTime()));
      case STRING:
        try {
          return new BsonDecimal128(Decimal128.parse(convertToString(bsonValue)));
        } catch (NumberFormatException e) {
          throw invalidFieldData(fieldName, dataType, bsonValue);
        }
      default:
        if (bsonValue instanceof BsonNumber) {
          return (BsonNumber) bsonValue;
        }
        throw invalidFieldData(fieldName, dataType, bsonValue);
    }
  }

  private String createFieldPath(final String currentLevel, final String subLevel) {
    return currentLevel.isEmpty() ? subLevel : format("%s.%s", currentLevel, subLevel);
  }

  private DataException invalidFieldData(
      final String fieldName, final DataType dataType, final BsonValue bsonValue) {
    return invalidFieldData(fieldName, dataType, bsonValue, "");
  }

  private DataException invalidFieldData(
      final String fieldName,
      final DataType dataType,
      final BsonValue bsonValue,
      final String extraMessage) {
    return new DataException(
        format(
            "Invalid field: '%s'. The dataType '%s' is invalid for '%s'.%s",
            fieldName, dataType.typeName(), bsonValue, extraMessage));
  }

  private DataException missingFieldException(final String fieldPath, final BsonDocument value) {
    return new DataException(
        format(
            "Missing field '%s' in: '%s'",
            fieldPath, value.toJson(ConverterHelper.DEFAULT_JSON_WRITER_SETTINGS)));
  }

  @VisibleForTesting
  static byte[] documentToByteArray(final BsonDocument document) {
    if (document instanceof RawBsonDocument) {
      RawBsonDocument rawBsonDocument = (RawBsonDocument) document;
      ByteBuffer byteBuffer = rawBsonDocument.getByteBuffer().asNIO();
      int startPosition = byteBuffer.position();
      int length = byteBuffer.limit() - startPosition;
      byte[] byteArray = new byte[length];
      System.arraycopy(byteBuffer.array(), startPosition, byteArray, 0, length);
      return byteArray;
    } else {
      BasicOutputBuffer buffer = new BasicOutputBuffer();
      try (BsonBinaryWriter writer = new BsonBinaryWriter(buffer)) {
        BSON_VALUE_CODEC.encode(writer, document, EncoderContext.builder().build());
      }
      return buffer.toByteArray();
    }
  }

  @Override
  public String toString() {
    return "BsonDocumentToRowConverter{" + "schema=" + schema + '}';
  }
}
