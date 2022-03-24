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

package com.mongodb.spark.sql.connector.read;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.sources.StringContains;
import org.apache.spark.sql.sources.StringEndsWith;
import org.apache.spark.sql.sources.StringStartsWith;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.Nullable;

import org.bson.BsonDocument;
import org.bson.conversions.Bson;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;

import com.mongodb.spark.sql.connector.assertions.Assertions;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.config.ReadConfig;

/** A builder for a {@link MongoScan}. */
public class MongoScanBuilder
    implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns {

  private final StructType schema;
  private final ReadConfig readConfig;
  private final boolean isCaseSensitive;
  private List<BsonDocument> datasetAggregationPipeline;
  private Filter[] pushedFilters;
  private StructType prunedSchema;

  /**
   * Construct a new instance
   *
   * @param schema the schema to use for the read
   * @param readConfig the configuration for the read
   */
  public MongoScanBuilder(final StructType schema, final ReadConfig readConfig) {
    this.schema = schema;
    this.readConfig = readConfig;
    this.prunedSchema = schema;
    this.isCaseSensitive =
        SparkSession.getActiveSession()
            .map(s -> s.sessionState().conf().caseSensitiveAnalysis())
            .getOrElse(() -> false);
    this.datasetAggregationPipeline = emptyList();
    this.pushedFilters = new Filter[0];
  }

  /** @return the {@link MongoScan} for the configured scan */
  @Override
  public Scan build() {
    List<BsonDocument> scanAggregationPipeline = new ArrayList<>();
    scanAggregationPipeline.addAll(readConfig.getAggregationPipeline());
    scanAggregationPipeline.addAll(datasetAggregationPipeline);

    ReadConfig scanReadConfig =
        readConfig.withOption(
            MongoConfig.READ_PREFIX + ReadConfig.AGGREGATION_PIPELINE_CONFIG,
            scanAggregationPipeline.stream()
                .map(BsonDocument::toJson)
                .collect(Collectors.joining(",", "[", "]")));
    return new MongoScan(prunedSchema, scanReadConfig);
  }

  /**
   * Processes filters on the dataset.
   *
   * <p>Sets any filters that can be pushed down into an aggregation `$match` pipeline stage.
   *
   * @param filters data filters
   * @return any filters for Spark to process
   */
  @Override
  public Filter[] pushFilters(final Filter[] filters) {
    List<FilterAndPipelineStage> processed =
        Arrays.stream(filters).map(this::processFilter).collect(Collectors.toList());

    List<FilterAndPipelineStage> withPipelines =
        processed.stream()
            .filter(FilterAndPipelineStage::hasPipelineStage)
            .collect(Collectors.toList());

    datasetAggregationPipeline =
        withPipelines.isEmpty()
            ? emptyList()
            : singletonList(
                Aggregates.match(
                        Filters.and(
                            withPipelines.stream()
                                .map(FilterAndPipelineStage::getPipelineStage)
                                .collect(Collectors.toList())))
                    .toBsonDocument());
    pushedFilters =
        withPipelines.stream().map(FilterAndPipelineStage::getFilter).toArray(Filter[]::new);

    return processed.stream()
        .filter(e -> !e.hasPipelineStage())
        .map(FilterAndPipelineStage::getFilter)
        .toArray(Filter[]::new);
  }

  /** @return any filters that have been converted into an aggregation pipeline. */
  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  @Override
  public void pruneColumns(final StructType requiredSchema) {
    Set<String> requiredColumns =
        Arrays.stream(requiredSchema.fields()).map(this::getColumnName).collect(Collectors.toSet());
    StructField[] fields =
        Arrays.stream(schema.fields())
            .filter(f -> requiredColumns.contains(getColumnName(f)))
            .toArray(StructField[]::new);
    prunedSchema = new StructType(fields);
  }

  private String getColumnName(final StructField field) {
    if (isCaseSensitive) {
      return field.name().toLowerCase(Locale.ROOT);
    }
    return field.name();
  }

  /**
   * Processes the Filter and if possible creates the equivalent aggregation pipeline stage.
   *
   * <p>To aid performance `IsNotNull` filters are not converted and left to Spark to process. In
   * most cases the pipeline `$ne: null` filter is not even needed as it is implicitly covered by
   * any extra filters.
   *
   * @param filter the filter to be applied
   * @return the FilterAndPipelineStage which contains a pipeline stage if the filter is convertible
   *     into an aggregation pipeline.
   */
  private FilterAndPipelineStage processFilter(final Filter filter) {
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
      return new FilterAndPipelineStage(
          filter, Filters.eq(equalNullSafe.attribute(), equalNullSafe.value()));
    } else if (filter instanceof EqualTo) {
      EqualTo equalTo = (EqualTo) filter;
      return new FilterAndPipelineStage(filter, Filters.eq(equalTo.attribute(), equalTo.value()));
    } else if (filter instanceof GreaterThan) {
      GreaterThan greaterThan = (GreaterThan) filter;
      return new FilterAndPipelineStage(
          filter, Filters.gt(greaterThan.attribute(), greaterThan.value()));
    } else if (filter instanceof GreaterThanOrEqual) {
      GreaterThanOrEqual greaterThanOrEqual = (GreaterThanOrEqual) filter;
      return new FilterAndPipelineStage(
          filter, Filters.gte(greaterThanOrEqual.attribute(), greaterThanOrEqual.value()));
    } else if (filter instanceof In) {
      In inFilter = (In) filter;
      return new FilterAndPipelineStage(
          filter, Filters.in(inFilter.attribute(), inFilter.values()));
    } else if (filter instanceof IsNull) {
      IsNull isNullFilter = (IsNull) filter;
      return new FilterAndPipelineStage(filter, Filters.eq(isNullFilter.attribute(), null));
    } else if (filter instanceof LessThan) {
      LessThan lessThan = (LessThan) filter;
      return new FilterAndPipelineStage(filter, Filters.lt(lessThan.attribute(), lessThan.value()));
    } else if (filter instanceof LessThanOrEqual) {
      LessThanOrEqual lessThanOrEqual = (LessThanOrEqual) filter;
      return new FilterAndPipelineStage(
          filter, Filters.lte(lessThanOrEqual.attribute(), lessThanOrEqual.value()));
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
      return new FilterAndPipelineStage(
          filter,
          Filters.regex(stringContains.attribute(), format(".*%s.*", stringContains.value())));
    } else if (filter instanceof StringEndsWith) {
      StringEndsWith stringEndsWith = (StringEndsWith) filter;
      return new FilterAndPipelineStage(
          filter,
          Filters.regex(stringEndsWith.attribute(), format(".*%s$", stringEndsWith.value())));
    } else if (filter instanceof StringStartsWith) {
      StringStartsWith stringStartsWith = (StringStartsWith) filter;
      return new FilterAndPipelineStage(
          filter,
          Filters.regex(stringStartsWith.attribute(), format("^%s.*", stringStartsWith.value())));
    }
    return new FilterAndPipelineStage(filter, null);
  }

  /** FilterAndPipelineStage - contains an optional pipeline stage for the filter. */
  private static final class FilterAndPipelineStage {

    private final Filter filter;
    private final Bson pipelineStage;

    private FilterAndPipelineStage(final Filter filter, @Nullable final Bson pipelineStage) {
      this.filter = filter;
      this.pipelineStage = pipelineStage;
    }

    public Filter getFilter() {
      return filter;
    }

    public Bson getPipelineStage() {
      return pipelineStage;
    }

    boolean hasPipelineStage() {
      return pipelineStage != null;
    }
  }
}
