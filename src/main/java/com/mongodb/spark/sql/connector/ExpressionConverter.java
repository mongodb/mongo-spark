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

import static com.mongodb.spark.sql.connector.schema.RowToBsonDocumentConverter.createObjectToBsonValue;
import static java.lang.String.format;

import com.mongodb.client.model.Filters;
import com.mongodb.spark.sql.connector.assertions.Assertions;
import com.mongodb.spark.sql.connector.config.WriteConfig;
import com.mongodb.spark.sql.connector.schema.RowToBsonDocumentConverter;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.sources.StringContains;
import org.apache.spark.sql.sources.StringEndsWith;
import org.apache.spark.sql.sources.StringStartsWith;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Utility class to convert {@link Filter} expressions into MongoDB aggregation pipelines
 *
 * @since 10.6
 */
public final class ExpressionConverter {
  private final StructType schema;

  /**
   * Construct a new instance
   * @param schema the schema for the data
   */
  public ExpressionConverter(final StructType schema) {
    this.schema = schema;
  }

  /**
   * Processes {@link Filter} into aggregation pipelines if possible
   * @param filter the filter to translate
   * @return the {@link FilterAndPipelineStage} representing the Filter and pipeline stage if conversion is possible
   */
  public FilterAndPipelineStage processFilter(final Filter filter) {
    Assertions.ensureArgument(() -> filter != null, () -> "Invalid argument filter cannot be null");
    if (filter instanceof And) {
      And andFilter = (And) filter;
      FilterAndPipelineStage eitherLeft = processFilter(andFilter.left());
      FilterAndPipelineStage eitherRight = processFilter(andFilter.right());
      if (eitherLeft.hasPipelineStage() && eitherRight.hasPipelineStage()) {
        return new FilterAndPipelineStage(
            filter, Filters.and(eitherLeft.getPipelineStage(), eitherRight.getPipelineStage()));
      }
    } else if (filter instanceof EqualNullSafe) {
      EqualNullSafe equalNullSafe = (EqualNullSafe) filter;
      String fieldName = unquoteFieldName(equalNullSafe.attribute());
      return new FilterAndPipelineStage(
          filter,
          getBsonValue(fieldName, equalNullSafe.value())
              .map(bsonValue -> Filters.eq(fieldName, bsonValue))
              .orElse(null));
    } else if (filter instanceof EqualTo) {
      EqualTo equalTo = (EqualTo) filter;
      String fieldName = unquoteFieldName(equalTo.attribute());
      return new FilterAndPipelineStage(
          filter,
          getBsonValue(fieldName, equalTo.value())
              .map(bsonValue -> Filters.eq(fieldName, bsonValue))
              .orElse(null));
    } else if (filter instanceof GreaterThan) {
      GreaterThan greaterThan = (GreaterThan) filter;
      String fieldName = unquoteFieldName(greaterThan.attribute());
      return new FilterAndPipelineStage(
          filter,
          getBsonValue(fieldName, greaterThan.value())
              .map(bsonValue -> Filters.gt(fieldName, bsonValue))
              .orElse(null));
    } else if (filter instanceof GreaterThanOrEqual) {
      GreaterThanOrEqual greaterThanOrEqual = (GreaterThanOrEqual) filter;
      String fieldName = unquoteFieldName(greaterThanOrEqual.attribute());
      return new FilterAndPipelineStage(
          filter,
          getBsonValue(fieldName, greaterThanOrEqual.value())
              .map(bsonValue -> Filters.gte(fieldName, bsonValue))
              .orElse(null));
    } else if (filter instanceof In) {
      In inFilter = (In) filter;
      String fieldName = unquoteFieldName(inFilter.attribute());
      List<BsonValue> values = Arrays.stream(inFilter.values())
          .map(v -> getBsonValue(fieldName, v))
          .filter(Optional::isPresent)
          .map(Optional::get)
          .collect(Collectors.toList());

      // Ensure all values were matched otherwise leave to Spark to filter.
      Bson pipelineStage = null;
      if (values.size() == inFilter.values().length) {
        pipelineStage = Filters.in(fieldName, values);
      }
      return new FilterAndPipelineStage(filter, pipelineStage);
    } else if (filter instanceof IsNull) {
      IsNull isNullFilter = (IsNull) filter;
      String fieldName = unquoteFieldName(isNullFilter.attribute());
      return new FilterAndPipelineStage(filter, Filters.eq(fieldName, null));
    } else if (filter instanceof IsNotNull) {
      IsNotNull isNotNullFilter = (IsNotNull) filter;
      String fieldName = unquoteFieldName(isNotNullFilter.attribute());
      return new FilterAndPipelineStage(filter, Filters.ne(fieldName, null));
    } else if (filter instanceof LessThan) {
      LessThan lessThan = (LessThan) filter;
      String fieldName = unquoteFieldName(lessThan.attribute());
      return new FilterAndPipelineStage(
          filter,
          getBsonValue(fieldName, lessThan.value())
              .map(bsonValue -> Filters.lt(fieldName, bsonValue))
              .orElse(null));
    } else if (filter instanceof LessThanOrEqual) {
      LessThanOrEqual lessThanOrEqual = (LessThanOrEqual) filter;
      String fieldName = unquoteFieldName(lessThanOrEqual.attribute());
      return new FilterAndPipelineStage(
          filter,
          getBsonValue(fieldName, lessThanOrEqual.value())
              .map(bsonValue -> Filters.lte(fieldName, bsonValue))
              .orElse(null));
    } else if (filter instanceof Not) {
      Not notFilter = (Not) filter;
      FilterAndPipelineStage notChild = processFilter(notFilter.child());
      if (notChild.hasPipelineStage()) {
        return new FilterAndPipelineStage(filter, Filters.not(notChild.pipelineStage));
      }
    } else if (filter instanceof Or) {
      Or or = (Or) filter;
      FilterAndPipelineStage eitherLeft = processFilter(or.left());
      FilterAndPipelineStage eitherRight = processFilter(or.right());
      if (eitherLeft.hasPipelineStage() && eitherRight.hasPipelineStage()) {
        return new FilterAndPipelineStage(
            filter, Filters.or(eitherLeft.getPipelineStage(), eitherRight.getPipelineStage()));
      }
    } else if (filter instanceof StringContains) {
      StringContains stringContains = (StringContains) filter;
      String fieldName = unquoteFieldName(stringContains.attribute());
      String literalValue = escapeRegex(stringContains.value());
      return new FilterAndPipelineStage(
          filter, Filters.regex(fieldName, format(".*%s.*", literalValue)));
    } else if (filter instanceof StringEndsWith) {
      StringEndsWith stringEndsWith = (StringEndsWith) filter;
      String fieldName = unquoteFieldName(stringEndsWith.attribute());
      String literalValue = escapeRegex(stringEndsWith.value());
      return new FilterAndPipelineStage(
          filter, Filters.regex(fieldName, format(".*%s$", literalValue)));
    } else if (filter instanceof StringStartsWith) {
      StringStartsWith stringStartsWith = (StringStartsWith) filter;
      String fieldName = unquoteFieldName(stringStartsWith.attribute());
      String literalValue = escapeRegex(stringStartsWith.value());
      return new FilterAndPipelineStage(
          filter, Filters.regex(fieldName, format("^%s.*", literalValue)));
    }
    return new FilterAndPipelineStage(filter, null);
  }

  private static String escapeRegex(String input) {
    return Pattern.quote(input);
  }

  @VisibleForTesting
  static String unquoteFieldName(final String fieldName) {
    // Spark automatically escapes hyphenated names using backticks
    if (fieldName.contains("`")) {
      return new Column(fieldName).toString();
    }
    return fieldName;
  }

  private Optional<BsonValue> getBsonValue(final String fieldName, final Object value) {
    try {
      StructType localSchema = schema;
      DataType localDataType = localSchema;

      for (String localFieldName : fieldName.split("\\.")) {
        StructField localField = localSchema.apply(localFieldName);
        localDataType = localField.dataType();
        if (localField.dataType() instanceof StructType) {
          localSchema = (StructType) localField.dataType();
        }
      }
      RowToBsonDocumentConverter.ObjectToBsonValue objectToBsonValue =
          createObjectToBsonValue(localDataType, WriteConfig.ConvertJson.FALSE, false);
      return Optional.of(objectToBsonValue.apply(value));
    } catch (Exception e) {
      // ignore
      return Optional.empty();
    }
  }

  /** FilterAndPipelineStage - contains an optional pipeline stage for the filter. */
  public static final class FilterAndPipelineStage {

    private final Filter filter;
    private final Bson pipelineStage;

    private FilterAndPipelineStage(final Filter filter, @Nullable final Bson pipelineStage) {
      this.filter = filter;
      this.pipelineStage = pipelineStage;
    }

    /**
     * @return the filter
     */
    public Filter getFilter() {
      return filter;
    }

    /**
     * @return the equivalent pipeline for the filter or {@code null} if translation for the filter wasn't possible
     */
    public Bson getPipelineStage() {
      return pipelineStage;
    }

    /**
     * @return true if the {@link Filter} could be converted into a pipeline stage
     */
    public boolean hasPipelineStage() {
      return pipelineStage != null;
    }
  }
}
