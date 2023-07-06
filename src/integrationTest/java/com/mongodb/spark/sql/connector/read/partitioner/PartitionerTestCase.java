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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.mongodb.MongoSparkConnectorTestCase;
import com.mongodb.spark.sql.connector.read.MongoInputPartition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

abstract class PartitionerTestCase extends MongoSparkConnectorTestCase {

  private static final StructType ID_ONLY_STRUCT =
      createStructType(singletonList(createStructField("_id", DataTypes.StringType, false)));

  private final ReadConfig readConfig;
  private final List<String> preferredLocations;

  PartitionerTestCase() {
    super();
    this.readConfig = getMongoConfig().toReadConfig();
    this.preferredLocations = PartitionerHelper.getPreferredLocations(readConfig);
  }

  List<String> getPreferredLocations() {
    return preferredLocations;
  }

  List<String> defaultReadConfigOptions() {
    return emptyList();
  }

  /**
   * Creates a read config using the varargs passed in.
   *
   * <p>Uses the varargs as key value pairs
   */
  ReadConfig createReadConfig(final String collectionName, final String... values) {
    Map<String, String> readConfigOptions = new HashMap<>();
    ArrayList<String> options = new ArrayList<>(defaultReadConfigOptions());
    options.add(ReadConfig.COLLECTION_NAME_CONFIG);
    options.add(collectionName);
    options.addAll(asList(values));
    assert (options.size() % 2 == 0);
    for (int i = 0; i < options.size(); i = i + 2) {
      readConfigOptions.put(ReadConfig.READ_PREFIX + options.get(i), options.get(i + 1));
    }
    return readConfig.withOptions(readConfigOptions);
  }

  /**
   * Checks the partitioner for correctness.
   *
   * <p>1. Checks the expected partitions match the generated partitions 2. Checks the partitioner
   * covers all the data in the collection
   */
  void assertPartitioner(
      final Partitioner partitioner,
      final List<MongoInputPartition> expectedPartitions,
      final ReadConfig readConfig) {
    assertIterableEquals(expectedPartitions, partitioner.generatePartitions(readConfig));
    assertPartitionerCoversAllData(partitioner, readConfig);
  }

  /**
   * Checks the partitioner covers all the data in the collection.
   *
   * <p>Also ensures that any user supplied pipelines are included
   */
  void assertPartitionerCoversAllData(final Partitioner partitioner, final ReadConfig readConfig) {
    SparkSession spark = getOrCreateSparkSession();
    List<String> actualIds = spark
        .read()
        .options(readConfig.getOptions())
        .option(ReadConfig.PARTITIONER_CONFIG, partitioner.getClass().getName())
        .schema(ID_ONLY_STRUCT)
        .format("mongodb")
        .load()
        .select("_id")
        .map((MapFunction<Row, String>) r -> r.getString(0), Encoders.STRING())
        .collectAsList();

    assertEquals(
        new HashSet<>(actualIds).size(), actualIds.size(), "Partitioner returns duplicated ids");

    List<String> expectedDocumentsIds = readConfig
        .withCollection(coll -> coll.aggregate(readConfig.getAggregationPipeline()))
        .map(d -> d.getString("_id").getValue())
        .into(new ArrayList<>());

    assertIterableEquals(expectedDocumentsIds, actualIds, () -> {
      List<String> missing = new ArrayList<>(expectedDocumentsIds);
      missing.removeAll(actualIds);
      if (missing.isEmpty()) {
        return null;
      }
      return "Missing ids: " + missing;
    });
  }
}
