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
import static java.util.Arrays.asList;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.json.JsonParseException;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import com.mongodb.spark.sql.connector.exceptions.DataException;
import com.mongodb.spark.sql.connector.interop.JavaScala;

/**
 * The helper for conversion of GenericRowWithSchema instances to BsonDocuments.
 *
 * <p>All Spark types are considered convertible to Bson types.
 */
@NotNull
public final class RowToBsonDocumentConverter implements Serializable {

  private static final long serialVersionUID = 1L;

  private final InternalRowToRowFunction internalRowToRowFunction;
  private final boolean convertJson;

  /**
   * Construct a new instance
   *
   * @param schema the schema for the row
   * @param convertJson the true if JSON strings should be converted
   */
  public RowToBsonDocumentConverter(final StructType schema, final boolean convertJson) {
    this.internalRowToRowFunction = new InternalRowToRowFunction(schema);
    this.convertJson = convertJson;
  }

  /**
   * Converts a {@link InternalRow} to a {@link BsonDocument}
   *
   * @param row the internal row to convert
   * @throws DataException if the {@code Row} does not have a schema associated with it
   * @return a BsonDocument representing the data in the row
   */
  public BsonDocument fromRow(final InternalRow row) {
    return fromRow(internalRowToRowFunction.apply(row));
  }

  /**
   * Converts a {@link Row} to a {@link BsonDocument}
   *
   * @param row the row to convert
   * @throws DataException if the {@code Row} does not have a schema associated with it
   * @return a BsonDocument representing the data in the row
   */
  public BsonDocument fromRow(final Row row) {
    if (row.schema() == null) {
      throw new DataException("Cannot convert Row without schema");
    }
    return toBsonValue(row.schema(), row).asDocument();
  }

  /**
   * Converts data to a bson value that the data type represents
   *
   * @param dataType the data type
   * @param data the data
   * @return the bsonValue
   */
  @SuppressWarnings("unchecked")
  public BsonValue toBsonValue(final DataType dataType, final Object data) {
    try {
      if (data == null) {
        return BsonNull.VALUE;
      } else if (DataTypes.BinaryType.acceptsType(dataType)) {
        return new BsonBinary((byte[]) data);
      } else if (DataTypes.BooleanType.acceptsType(dataType)) {
        return new BsonBoolean((Boolean) data);
      } else if (DataTypes.DoubleType.acceptsType(dataType)) {
        return new BsonDouble(((Number) data).doubleValue());
      } else if (DataTypes.FloatType.acceptsType(dataType)) {
        return new BsonDouble(((Number) data).floatValue());
      } else if (DataTypes.IntegerType.acceptsType(dataType)) {
        return new BsonInt32(((Number) data).intValue());
      } else if (DataTypes.ShortType.acceptsType(dataType)) {
        return new BsonInt32(((Number) data).intValue());
      } else if (DataTypes.ByteType.acceptsType(dataType)) {
        return new BsonInt32(((Number) data).intValue());
      } else if (DataTypes.LongType.acceptsType(dataType)) {
        return new BsonInt64(((Number) data).longValue());
      } else if (DataTypes.StringType.acceptsType(dataType)) {
        return processString((String) data);
      } else if (DataTypes.DateType.acceptsType(dataType)
          || DataTypes.TimestampType.acceptsType(dataType)) {
        return new BsonDateTime(((Date) data).getTime());
      } else if (DataTypes.NullType.acceptsType(dataType)) {
        return BsonNull.VALUE;
      } else if (dataType instanceof DecimalType) {
        BigDecimal bigDecimal =
            data instanceof BigDecimal
                ? (BigDecimal) data
                : ((Decimal) data).toBigDecimal().bigDecimal();
        return new BsonDecimal128(new Decimal128(bigDecimal));
      } else if (dataType instanceof ArrayType) {
        DataType elementType = ((ArrayType) dataType).elementType();
        BsonArray bsonArray = new BsonArray();

        List<Object> listData;
        if (data instanceof List) {
          listData = (List<Object>) data;
        } else if (data instanceof Object[]) {
          listData = asList((Object[]) data);
        } else {
          listData = JavaScala.asJava((scala.collection.Seq<Object>) data);
        }
        listData.forEach(d -> bsonArray.add(toBsonValue(elementType, d)));
        return bsonArray;
      } else if (dataType instanceof MapType) {
        DataType keyType = ((MapType) dataType).keyType();
        DataType valueType = ((MapType) dataType).valueType();
        if (!(keyType instanceof StringType)) {
          throw new DataException(
              format("Cannot cast %s into a BsonValue. Invalid key type %s.", data, keyType));
        }
        BsonDocument bsonDocument = new BsonDocument();
        Map<String, Object> mapData;
        if (data instanceof Map) {
          mapData = (Map<String, Object>) data;
        } else {
          mapData = JavaScala.asJava((scala.collection.Map<String, Object>) data);
        }
        mapData.forEach((k, v) -> bsonDocument.put(k, toBsonValue(valueType, v)));
        return bsonDocument;
      } else if (dataType instanceof StructType) {
        Row row = (Row) data;
        BsonDocument bsonDocument = new BsonDocument();
        StructField[] fields = row.schema().fields();
        if (fields.length == 1 && fields[0].name().equals("oid")) {
          return new BsonObjectId(new ObjectId(row.getString(0)));
        }
        for (StructField field : row.schema().fields()) {
          int fieldIndex = row.fieldIndex(field.name());
          bsonDocument.append(field.name(), toBsonValue(field.dataType(), row.get(fieldIndex)));
        }
        return bsonDocument;
      }
    } catch (Exception e) {
      throw new DataException(
          format(
              "Cannot cast %s into a BsonValue. %s has no matching BsonValue. Error: %s",
              data, dataType, e.getMessage()));
    }

    throw new DataException(
        format(
            "Cannot cast %s into a BsonValue. %s data type has no matching BsonValue.",
            data, dataType));
  }

  private static final String BSON_TEMPLATE = "{v: %s}";

  private BsonValue processString(final String data) {
    if (convertJson) {
      try {
        return BsonDocument.parse(format(BSON_TEMPLATE, data)).get("v");
      } catch (JsonParseException e) {
        return new BsonString(data);
      }
    }
    return new BsonString(data);
  }
}
