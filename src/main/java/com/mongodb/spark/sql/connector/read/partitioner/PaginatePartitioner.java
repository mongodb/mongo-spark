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

package com.mongodb.spark.sql.connector.read.partitioner;

import java.util.ArrayList;
import java.util.List;

import org.jetbrains.annotations.ApiStatus;

import org.bson.BsonDocument;
import org.bson.conversions.Bson;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;

import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.read.MongoInputPartition;

/**
 * Paginates a collection into partitions.
 *
 * <ul>
 *   <li>{@value PARTITION_FIELD_LIST_CONFIG}: A comma delimited list of fields to be used for
 *       partitioning. Defaults to: {@value ID_FIELD}.
 * </ul>
 *
 * <p>Note: This method of partitioning costs at least on aggregation query per partition produced.
 */
@ApiStatus.Internal
abstract class PaginatePartitioner extends FieldListPartitioner {

    /**
     * Uses the collection count and the number of documents per partition to create the {@code MongoInputPartition}s.
     *
     * @param count the count of documents in the collection after any user provided aggregations are applied.
     * @param numDocumentsPerPartition the calculated number of documents per partition
     * @param readConfig the read configuration
     * @return a list of {@link MongoInputPartition}s.
     */
  List<MongoInputPartition> createMongoInputPartitions(
      final long count, final int numDocumentsPerPartition, final ReadConfig readConfig) {
    List<String> partitionFieldList = getPartitionFieldList(readConfig);
    return createMongoInputPartitions(
        partitionFieldList,
        createUpperBounds(partitionFieldList, count, numDocumentsPerPartition, readConfig),
        readConfig);
  }

    /**
     * Creates an ordered list of the upper boundaries for each partition.
     *
     * <p>Calculates the number of partitions to be {@code Math.ceil(count / numDocumentsPerPartition)} and each partition requires a query.
     *
     * @param partitionFieldList partitionFieldList the fields to be used in partitioning each partition
     * @param count the count of documents in the collection after any user provided aggregations are applied.
     * @param numDocumentsPerPartition the calculated number of documents per partition
     * @param readConfig the read configuration
     * @return an ordered list of documents representing the upper bounds for each partition.
     */
  private List<BsonDocument> createUpperBounds(
      final List<String> partitionFieldList,
      final long count,
      final int numDocumentsPerPartition,
      final ReadConfig readConfig) {

    // Calculate partition ranges
    int numberOfPartitions = (int) Math.ceil(count / (double) numDocumentsPerPartition);

    List<BsonDocument> upperBounds = new ArrayList<>();
    for (int i = 0; i < numberOfPartitions; i++) {
      int skip =
          ((long) numDocumentsPerPartition * i <= count)
              ? numDocumentsPerPartition
              : (int) (count - numDocumentsPerPartition * i);

      Bson projection =
          partitionFieldList.contains(ID_FIELD)
              ? Projections.include(partitionFieldList)
              : Projections.fields(
                  Projections.include(partitionFieldList), Projections.excludeId());

      List<Bson> aggregationPipeline = new ArrayList<>(readConfig.getAggregationPipeline());
      aggregationPipeline.add(Aggregates.project(projection));
      aggregationPipeline.add(Aggregates.sort(Sorts.ascending(partitionFieldList)));

      BsonDocument boundary =
          readConfig.withCollection(
              coll -> {
                List<Bson> boundaryPipeline = new ArrayList<>();

                // Uses the previous boundary as the $gte match to efficiently skip to the next bounds.
                if (!upperBounds.isEmpty()) {
                  BsonDocument previous = upperBounds.get(upperBounds.size() - 1);
                  BsonDocument matchFilter = new BsonDocument();
                  partitionFieldList.forEach(
                      k -> {
                        if (previous.containsKey(k)) {
                          matchFilter.put(k, new BsonDocument("$gte", previous.get(k)));
                        }
                      });
                  boundaryPipeline.add(Aggregates.match(matchFilter));
                }
                boundaryPipeline.addAll(aggregationPipeline);
                boundaryPipeline.add(Aggregates.skip(skip));
                boundaryPipeline.add(Aggregates.limit(1));
               return coll.aggregate(boundaryPipeline).first();
              });

      if (boundary == null) {
        break;
      }
      upperBounds.add(boundary);
    }
    return upperBounds;
  }
}
