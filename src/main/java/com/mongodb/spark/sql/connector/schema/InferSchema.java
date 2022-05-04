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
import static java.util.stream.Collectors.groupingBy;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.spark.sql.catalyst.analysis.TypeCoercion;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import com.mongodb.client.model.Aggregates;

import com.mongodb.spark.sql.connector.assertions.Assertions;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.config.ReadConfig;

/**
 * A helper that determines the {@code StructType} for a {@code BsonDocument} and finds the common
 * {@code StructType} for a list of BsonDocuments.
 *
 * <p>All Bson types are considered convertible to Spark types. For any Bson types that doesn't have
 * a direct conversion to a Spark type then a String type will be used.
 */
@NotNull
public final class InferSchema {

  /**
   * Infer the schema for the collection
   *
   * @param options the configuration options to determine the namespace to determine the schema for
   * @return the schema
   */
  public static StructType inferSchema(final CaseInsensitiveStringMap options) {
    ReadConfig readConfig =
        MongoConfig.readConfig(options.asCaseSensitiveMap())
            .withOptions(options.asCaseSensitiveMap());
    ArrayList<Bson> samplePipeline = new ArrayList<>(readConfig.getAggregationPipeline());
    samplePipeline.add(Aggregates.sample(readConfig.getInferSchemaSampleSize()));
    return inferSchema(
        readConfig.withCollection(coll -> coll.aggregate(samplePipeline).into(new ArrayList<>())),
        readConfig);
  }

  @VisibleForTesting
  static StructType inferSchema(
      final List<BsonDocument> bsonDocuments, final ReadConfig readConfig) {
    return bsonDocuments.stream()
        .map(d -> getStructType(d, readConfig))
        .reduce(PLACE_HOLDER_STRUCT_TYPE, (dt1, dt2) -> compatibleStructType(dt1, dt2, readConfig));
  }

  @NotNull
  private static StructType getStructType(
      final BsonDocument bsonDocument, final ReadConfig readConfig) {
    return (StructType) getDataType(bsonDocument, readConfig);
  }

  @VisibleForTesting
  static DataType getDataType(final BsonValue bsonValue, final ReadConfig readConfig) {
    switch (bsonValue.getBsonType()) {
      case DOCUMENT:
        List<StructField> fields = new ArrayList<>();
        bsonValue
            .asDocument()
            .forEach(
                (k, v) ->
                    fields.add(
                        new StructField(k, getDataType(v, readConfig), true, Metadata.empty())));
        return dataTypeCheckStructTypeToMapType(DataTypes.createStructType(fields), readConfig);
      case ARRAY:
        DataType elementType =
            bsonValue.asArray().stream()
                .map(v -> getDataType(v, readConfig))
                .distinct()
                .reduce((d1, d2) -> compatibleType(d1, d2, readConfig))
                .orElse(DataTypes.StringType);
        return DataTypes.createArrayType(elementType, true);
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
        return DataTypes.createDecimalType(
            Math.max(bigDecimal.precision(), bigDecimal.scale()), bigDecimal.scale());
      default:
        return DataTypes.StringType;
    }
  }

  /**
   * Determines the compatible {@link StructType} for the two StructTypes with the supplied {@link
   * ReadConfig}.
   *
   * <p>Uses {@link #compatibleType(DataType, DataType, ReadConfig)} to determine the compatible
   * data types for fields.
   *
   * @param structType1 the first StructType
   * @param structType2 the second StructType
   * @param readConfig the read configuration
   * @return the compatible {@link StructType} with the fields sorted by name or errors.
   */
  private static StructType compatibleStructType(
      final StructType structType1, final StructType structType2, final ReadConfig readConfig) {

    if (structType1 == PLACE_HOLDER_STRUCT_TYPE) {
      return structType2;
    }

    Map<String, List<StructField>> fieldNameToStructFieldMap =
        Stream.of(structType1.fields(), structType2.fields())
            .flatMap(Stream::of)
            .collect(groupingBy(StructField::name));

    List<StructField> structFields = new ArrayList<>();
    fieldNameToStructFieldMap.forEach(
        (fieldName, groupedStructFields) -> {
          DataType fieldCommonDataType =
              groupedStructFields.stream()
                  .map(StructField::dataType)
                  .reduce(
                      PLACE_HOLDER_DATA_TYPE, (dt1, dt2) -> compatibleType(dt1, dt2, readConfig));
          structFields.add(DataTypes.createStructField(fieldName, fieldCommonDataType, true));
        });

    structFields.sort(Comparator.comparing(StructField::name));
    return DataTypes.createStructType(structFields);
  }

  /**
   * Returns the compatible type between two data types. All types are can be converted to be
   * compatible because {@link DataTypes#StringType} is the lowest common type for all Bson types.
   *
   * <p>Uses the {@link TypeCoercion#findTightestCommonType()} function to find the most compatible
   * type. If the types are deemed incompatible because they are:
   *
   * <ul>
   *   <li>both Structs: then the Struct fields are merged and the types expanded until they are
   *       compatible. See {@link #compatibleStructType}.
   *   <li>both Arrays: the Array value types are expanded until they are compatible. See {@link
   *       #compatibleArrayType}.
   *   <li>both Decimals: creates a compatible type for the decimals. See {@link
   *       #compatibleDecimalType}.
   *   <li>A Map and Struct type: creates a new Map type with a compatible value type. See {@link
   *       #appendStructToMap}.
   * </ul>
   *
   * @param dataType1 the first data type
   * @param dataType2 the second data type
   * @param readConfig the read configuration
   * @return the compatible type
   */
  private static DataType compatibleType(
      @Nullable final DataType dataType1, final DataType dataType2, final ReadConfig readConfig) {

    if (dataType1 == PLACE_HOLDER_DATA_TYPE) {
      return dataType2;
    }

    DataType dataType =
        TypeCoercion.findTightestCommonType()
            .apply(dataType1, dataType2)
            .getOrElse(
                // dataType1 or dataType2 is a StructType, MapType, ArrayType or a decimal type.
                () -> {
                  if (dataType1 instanceof StructType && dataType2 instanceof StructType) {
                    return compatibleStructType(
                        (StructType) dataType1, (StructType) dataType2, readConfig);
                  }

                  if (dataType1 instanceof ArrayType && dataType2 instanceof ArrayType) {
                    return compatibleArrayType(
                        (ArrayType) dataType1, (ArrayType) dataType2, readConfig);
                  }

                  // The case that given `DecimalType` is capable of given `IntegralType` is handled
                  // in `findTightestCommonTypeOfTwo`. Both cases below will be executed only when
                  // the given `DecimalType` is not capable of the given `IntegralType`.
                  if (dataType1 instanceof DecimalType || dataType2 instanceof DecimalType) {
                    return compatibleDecimalType(dataType1, dataType2);
                  }

                  // If working with MapTypes and StructTypes append the struct data
                  if ((dataType1 instanceof MapType && dataType2 instanceof StructType)
                      || (dataType1 instanceof StructType && dataType2 instanceof MapType)) {
                    return appendStructToMap(dataType1, dataType2, readConfig);
                  }

                  return DataTypes.StringType; // Lowest common type
                });

    return dataTypeCheckStructTypeToMapType(dataType, readConfig);
  }

  private static DataType compatibleArrayType(
      final ArrayType arrayType1, final ArrayType arrayType2, final ReadConfig readConfig) {
    return DataTypes.createArrayType(
        compatibleType(arrayType1.elementType(), arrayType2.elementType(), readConfig),
        arrayType1.containsNull() || arrayType2.containsNull());
  }

  private static DataType appendStructToMap(
      final DataType dataType1, final DataType dataType2, final ReadConfig readConfig) {
    Assertions.ensureArgument(
        () ->
            (dataType1 instanceof StructType && dataType2 instanceof MapType)
                || (dataType1 instanceof MapType && dataType2 instanceof StructType),
        () ->
            format(
                "Requires a StructType and a MapType.  Got: %s, %s",
                dataType1.typeName(), dataType2.typeName()));
    StructType structType =
        dataType1 instanceof StructType ? (StructType) dataType1 : (StructType) dataType2;
    MapType mapType = dataType1 instanceof StructType ? (MapType) dataType2 : (MapType) dataType1;

    DataType valueType =
        Stream.concat(
                Stream.of(mapType.valueType()),
                Arrays.stream(structType.fields()).map(StructField::dataType))
            .reduce(PLACE_HOLDER_DATA_TYPE, (dt1, dt2) -> compatibleType(dt1, dt2, readConfig));
    return DataTypes.createMapType(mapType.keyType(), valueType, mapType.valueContainsNull());
  }

  private static DataType compatibleDecimalType(
      final DataType dataType1, final DataType dataType2) {
    Assertions.ensureArgument(
        () -> dataType1 instanceof DecimalType || dataType2 instanceof DecimalType,
        () ->
            format(
                "Neither datatype is an instance of DecimalType.  Got: %s, %s",
                dataType1.typeName(), dataType2.typeName()));
    DecimalType decimalType =
        dataType1 instanceof DecimalType ? (DecimalType) dataType1 : (DecimalType) dataType2;
    DataType dataType = dataType1 instanceof DecimalType ? dataType2 : dataType1;
    if (dataType instanceof DecimalType) {
      DecimalType decimalType2 = (DecimalType) dataType;
      int scale = Math.max(decimalType.scale(), decimalType2.scale());
      int range =
          Math.max(
              decimalType.precision() - decimalType.scale(),
              decimalType2.precision() - decimalType2.scale());

      if (range + scale > 38) {
        // DecimalType can't support precision > 38
        return DataTypes.DoubleType;
      } else {
        return DataTypes.createDecimalType(range + scale, scale);
      }
    } else if (dataType instanceof IntegerType) {
      return DataTypes.createDecimalType(10, 0);
    } else if (dataType instanceof LongType) {
      return DataTypes.createDecimalType(20, 0);
    } else if (dataType instanceof DoubleType) {
      return DataTypes.createDecimalType(30, 15);
    }
    return DataTypes.StringType;
  }

  private static DataType dataTypeCheckStructTypeToMapType(
      final DataType dataType, final ReadConfig readConfig) {
    if (dataType instanceof StructType) {
      StructType structType = (StructType) dataType;
      if (readConfig.inferSchemaMapType()
          && structType.fields().length >= readConfig.getInferSchemaMapTypeMinimumKeySize()) {
        DataType valueType =
            Arrays.stream(structType.fields())
                .map(StructField::dataType)
                .reduce(PLACE_HOLDER_DATA_TYPE, (dt1, dt2) -> compatibleType(dt1, dt2, readConfig));
        return DataTypes.createMapType(DataTypes.StringType, valueType, true);
      }
    }
    return dataType;
  }

  private static final StructType PLACE_HOLDER_STRUCT_TYPE =
      DataTypes.createStructType(new StructField[0]);

  private static final DataType PLACE_HOLDER_DATA_TYPE =
      new DataType() {
        @Override
        public int defaultSize() {
          return 0;
        }

        @Override
        public DataType asNullable() {
          return PLACE_HOLDER_DATA_TYPE;
        }
      };

  private InferSchema() {}
}
