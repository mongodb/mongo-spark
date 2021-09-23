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

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
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
import org.bson.codecs.BsonValueCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.Decimal128;

import com.mongodb.spark.sql.connector.exceptions.DataException;

/**
 * The helper for conversion of BsonDocuments to GenericRowWithSchema instances.
 *
 * <p>All Bson types are considered convertible to Spark types. For any Bson types that don't have a
 * direct conversion to a Spark type then a String type will be used.
 *
 * <p>When a {@link DataType} is provided by Spark, the conversion process will try to convert the
 * Bson type into the declared {@code DataType}.
 */
@NotNull
public class BsonDocumentToRowConverter {

  private static final Codec<BsonValue> BSON_VALUE_CODEC = new BsonValueCodec();
  private static final JsonWriterSettings DEFAULT_JSON_WRITER_SETTINGS =
      JsonWriterSettings.builder()
          .outputMode(JsonMode.RELAXED)
          .binaryConverter(
              (value, writer) ->
                  writer.writeString(Base64.getEncoder().encodeToString(value.getData())))
          .dateTimeConverter(
              (value, writer) -> {
                ZonedDateTime zonedDateTime = Instant.ofEpochMilli(value).atZone(ZoneOffset.UTC);
                writer.writeString(DateTimeFormatter.ISO_DATE_TIME.format(zonedDateTime));
              })
          .decimal128Converter((value, writer) -> writer.writeString(value.toString()))
          .objectIdConverter((value, writer) -> writer.writeString(value.toHexString()))
          .symbolConverter((value, writer) -> writer.writeString(value))
          .build();

  private final JsonWriterSettings jsonWriterSettings;

  /** Create a new instance with the default json writer settings */
  public BsonDocumentToRowConverter() {
    this(DEFAULT_JSON_WRITER_SETTINGS);
  }

  /**
   * Create a new instance
   *
   * @param jsonWriterSettings the {@link JsonWriterSettings} to use
   */
  public BsonDocumentToRowConverter(final JsonWriterSettings jsonWriterSettings) {
    this.jsonWriterSettings = jsonWriterSettings;
  }

  /**
   * Converts a {@code BsonDocument} into a {@link GenericRowWithSchema}. Uses the {@linkplain
   * BsonValue#getBsonType BSON types} of values in the {@code bsonDocument} to determine the {@code
   * StructType}
   *
   * @param bsonDocument the bson document to shape into a GenericRowWithSchema.
   * @return the converted data as a Row
   */
  public Row fromBsonDocument(final BsonDocument bsonDocument) {
    return fromBsonDocument((StructType) getDataType(bsonDocument), bsonDocument);
  }

  /**
   * Converts a {@code BsonDocument} into a {@link GenericRowWithSchema} using the supplied {@code
   * StructType}
   *
   * @param structType the schema to use
   * @param bsonDocument the bson document to shape into a GenericRowWithSchema.
   * @return the converted data as a Row
   */
  public Row fromBsonDocument(final StructType structType, final BsonDocument bsonDocument) {
    return convertToRow("", structType, bsonDocument);
  }

  @VisibleForTesting
  DataType getDataType(final BsonValue bsonValue) {
    switch (bsonValue.getBsonType()) {
      case DOCUMENT:
        List<StructField> fields = new ArrayList<>();
        bsonValue
            .asDocument()
            .forEach(
                (k, v) -> fields.add(new StructField(k, getDataType(v), true, Metadata.empty())));
        return DataTypes.createStructType(fields);
      case ARRAY:
        List<DataType> dataTypes =
            bsonValue.asArray().stream()
                .map(this::getDataType)
                .distinct()
                .collect(Collectors.toList());
        return DataTypes.createArrayType(simplifyDataTypes(dataTypes), true);
      case SYMBOL:
      case STRING:
      case OBJECT_ID:
        return DataTypes.StringType;
      case BINARY:
        return DataTypes.BinaryType;
      case BOOLEAN:
        return DataTypes.BooleanType;
      case TIMESTAMP:
      case DATE_TIME:
        return DataTypes.TimestampType;
      case NULL:
        return DataTypes.NullType;
      case DOUBLE:
        return DataTypes.DoubleType;
      case INT32:
        return DataTypes.IntegerType;
      case INT64:
        return DataTypes.LongType;
      case DECIMAL128:
        BigDecimal bigDecimal = bsonValue.asDecimal128().decimal128Value().bigDecimalValue();
        return DataTypes.createDecimalType(bigDecimal.precision(), bigDecimal.scale());
      default:
        return DataTypes.StringType;
    }
  }

  /**
   * Create the common datatype for an array
   *
   * <p>{@code DataTypes.String} is the common (lowest) type for conversion. Arrays containing
   * {@code StructType}s with the same field names, will also be processed to find the commonest
   * type for the field.
   *
   * @param dataTypes the data types contained by the array
   * @return the simplified DataType
   */
  private DataType simplifyDataTypes(final List<DataType> dataTypes) {
    return dataTypes.stream()
        .reduce(
            (dataType1, dataType2) -> {
              if (dataType1 instanceof StructType && dataType2 instanceof StructType) {
                StructType structType1 = (StructType) dataType1;
                StructType structType2 = (StructType) dataType2;
                if (Arrays.equals(structType1.fieldNames(), structType2.fieldNames())) {
                  List<StructField> simplifiedFields = new ArrayList<>();
                  StructField[] fields1 = structType1.fields();
                  StructField[] fields2 = structType2.fields();
                  for (int i = 0; i < fields1.length; i++) {
                    StructField structField = fields1[i];
                    if (structField.dataType().acceptsType(fields2[i].dataType())) {
                      simplifiedFields.add(structField);
                    } else {
                      simplifiedFields.add(
                          DataTypes.createStructField(
                              structField.name(), DataTypes.StringType, true));
                    }
                  }
                  return DataTypes.createStructType(simplifiedFields);
                }
              } else if (dataType1 instanceof ArrayType && dataType2 instanceof ArrayType) {
                return DataTypes.createArrayType(DataTypes.StringType, true);
              }
              return DataTypes.StringType;
            })
        .orElse(DataTypes.StringType);
  }

  @VisibleForTesting
  Object convertBsonValue(
      final String fieldName, final DataType dataType, final BsonValue bsonValue) {
    if (dataType instanceof StructType) {
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
      return convertToDate(fieldName, dataType, bsonValue);
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

  private Map<String, ?> convertToMap(
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

    return map;
  }

  private List<?> convertToArray(
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
    return arrayList;
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

  private Date convertToDate(
      final String fieldName, final DataType dataType, final BsonValue bsonValue) {
    return Date.from(convertToInstant(fieldName, dataType, bsonValue).truncatedTo(ChronoUnit.DAYS));
  }

  private Date convertToTimestamp(
      final String fieldName, final DataType dataType, final BsonValue bsonValue) {
    return Date.from(convertToInstant(fieldName, dataType, bsonValue));
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
        return bsonValue.asDocument().toJson(jsonWriterSettings);
      default:
        String value = new BsonDocument("v", bsonValue).toJson(jsonWriterSettings);
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

  private Instant convertToInstant(
      final String fieldName, final DataType dataType, final BsonValue bsonValue) {
    return Instant.ofEpochMilli(convertToLong(fieldName, dataType, bsonValue));
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
        format("Missing field '%s' in: '%s'", fieldPath, value.toJson(jsonWriterSettings)));
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
}
