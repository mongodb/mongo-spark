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

import static com.mongodb.spark.sql.connector.read.partitioner.AutoBucketPartitioner.createBucketAutoPipeline;
import static com.mongodb.spark.sql.connector.read.partitioner.AutoBucketPartitioner.createMongoInputPartitions;
import static com.mongodb.spark.sql.connector.read.partitioner.AutoBucketPartitioner.createPartitionPipeline;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.mongodb.spark.sql.connector.read.MongoInputPartition;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

public class AutoBucketPartitionerUnitTest {
  private static final BsonDocument EMPTY_DOCUMENT = new BsonDocument();

  @Test
  void testCreateBucketAutoPipeline() {
    // When no users pipeline and a single field
    assertPipeline(
        asList(
            "{\"$sample\": {\"size\": 1000}}",
            "{\"$bucketAuto\": {\"groupBy\": \"$a\", \"buckets\": 10}}"),
        createBucketAutoPipeline(EMPTY_DOCUMENT, singletonList("a"), "__idx", 1000, 10));

    // When no users pipeline and a multiple fields
    assertPipeline(
        asList(
            "{\"$sample\": {\"size\": 101}}",
            "{\"$addFields\": {\"__ab\": {\"0\": \"$a\", \"1\": \"$b\"}}}",
            "{\"$bucketAuto\": {\"groupBy\": \"$__ab\", \"buckets\": 5}}"),
        createBucketAutoPipeline(EMPTY_DOCUMENT, asList("a", "b"), "__ab", 101, 5));

    // When no users pipeline and dotted fields
    assertPipeline(
        asList(
            "{\"$sample\": {\"size\": 99}}",
            "{\"$addFields\": {\"__abc\": {\"0\": \"$a.b\", \"1\": \"$c\"}}}",
            "{\"$bucketAuto\": {\"groupBy\": \"$__abc\", \"buckets\": 9}}"),
        createBucketAutoPipeline(EMPTY_DOCUMENT, asList("a.b", "c"), "__abc", 99, 9));

    // With users pipeline
    assertPipeline(
        asList(
            "{\"$match\": {\"b\": {\"$gte\": 99}}}",
            "{\"$sample\": {\"size\": 1000}}",
            "{\"$bucketAuto\": {\"groupBy\": \"$a\", \"buckets\": 10}}"),
        createBucketAutoPipeline(
            BsonDocument.parse("{\"b\": {\"$gte\": 99}}"), singletonList("a"), "a", 1000, 10));
  }

  @Test
  void testCreatePartitionPipeline() {
    // Single field tests
    List<String> singleFieldList = singletonList("a");
    List<String> multipleFieldList = asList("a.b", "c");
    BsonDocument bounds = BsonDocument.parse("{\"$match\": {\"a\": {\"$gte\": 20, \"$lt\": 30}}}");
    BsonDocument complexBounds = BsonDocument.parse(
        "{\"$match\": {\"_abc\": {\"$gte\": {\"0\": 20, \"1\": \"z\"}, \"$lt\": {\"0\": 30, \"1\": \"a\"}}}}");
    List<BsonDocument> usersPipeline =
        toBsonDocuments("{\"$match\": {\"b\": {\"$gte\": 99}}}", "{\"$project\": {\"_id\": 0}}");

    assertPipeline(
        singletonList("{\"$match\": {\"a\": {\"$gte\": 20, \"$lt\": 30}}}"),
        createPartitionPipeline(singleFieldList, "__a", bounds, emptyList()));

    assertPipeline(
        asList(
            "{\"$match\": {\"a\": {\"$gte\": 20, \"$lt\": 30}}}",
            "{\"$match\": {\"b\": {\"$gte\": 99}}}",
            "{\"$project\": {\"_id\": 0}}"),
        createPartitionPipeline(singleFieldList, "__a", bounds, usersPipeline));

    // multiple field tests
    assertPipeline(
        asList(
            "{\"$addFields\": {\"_abc\": {\"0\": \"$a.b\", \"1\": \"$c\"}}}",
            "{\"$match\": {\"_abc\": {\"$gte\": {\"0\": 20, \"1\": \"z\"}, \"$lt\": {\"0\": 30, \"1\": \"a\"}}}}",
            "{\"$unset\": \"_abc\"}"),
        createPartitionPipeline(multipleFieldList, "_abc", complexBounds, emptyList()));

    assertPipeline(
        asList(
            "{\"$addFields\": {\"_abc\": {\"0\": \"$a.b\", \"1\": \"$c\"}}}",
            "{\"$match\": {\"_abc\": {\"$gte\": {\"0\": 20, \"1\": \"z\"}, \"$lt\": {\"0\": 30, \"1\": \"a\"}}}}",
            "{\"$unset\": \"_abc\"}",
            "{\"$match\": {\"b\": {\"$gte\": 99}}}",
            "{\"$project\": {\"_id\": 0}}"),
        createPartitionPipeline(multipleFieldList, "_abc", complexBounds, usersPipeline));
  }

  @Test
  void testCreateMongoInputPartitions() {
    List<String> singleFieldList = singletonList("a");
    List<String> multipleFieldList = asList("a.b", "c");
    List<BsonDocument> usersPipeline = toBsonDocuments("{\"$match\": {\"b\": {\"$gte\": 99}}}");

    List<String> preferredLocations = asList("a.example.com", "b.example.com");

    // Single field / no pipeline / no location
    assertIterableEquals(
        asList(
            new MongoInputPartition(
                0, toBsonDocuments("{\"$match\": {\"a\": { \"$lt\": 250}}}"), emptyList()),
            new MongoInputPartition(
                1,
                toBsonDocuments("{\"$match\": {\"a\": { \"$gte\": 250, \"$lt\": 500}}}"),
                emptyList()),
            new MongoInputPartition(
                2, toBsonDocuments("{\"$match\": {\"a\": { \"$gte\": 500}}}"), emptyList())),
        createMongoInputPartitions(
            toBsonDocuments(
                "{\"_id\": {\"min\": 0, \"max\": 250}}",
                "{\"_id\": {\"min\": 250, \"max\": 500}}",
                "{\"_id\": {\"min\": 500, \"max\": 750}}"),
            emptyList(),
            singleFieldList,
            "__idx",
            emptyList()));

    // Single field with pipeline & location
    assertIterableEquals(
        asList(
            new MongoInputPartition(
                0,
                toBsonDocuments(
                    "{\"$match\": {\"a\": { \"$lt\": 250}}}",
                    "{\"$match\": {\"b\": {\"$gte\": 99}}}"),
                preferredLocations),
            new MongoInputPartition(
                1,
                toBsonDocuments(
                    "{\"$match\": {\"a\": { \"$gte\": 250}}}",
                    "{\"$match\": {\"b\": {\"$gte\": 99}}}"),
                preferredLocations)),
        createMongoInputPartitions(
            toBsonDocuments(
                "{\"_id\": {\"min\": 0, \"max\": 250}}", "{\"_id\": {\"min\": 250, \"max\": 500}}"),
            usersPipeline,
            singleFieldList,
            "__idx",
            preferredLocations));

    // multiple field list with pipeline & location
    assertIterableEquals(
        asList(
            new MongoInputPartition(
                0,
                toBsonDocuments(
                    "{\"$addFields\": {\"__idx\": {\"0\": \"$a.b\", \"1\": \"$c\"}}}",
                    "{\"$match\": {\"__idx\": { \"$lt\": {\"0\": 250, \"1\": \"z\"}}}}",
                    "{\"$unset\": \"__idx\"}",
                    "{\"$match\": {\"b\": {\"$gte\": 99}}}"),
                preferredLocations),
            new MongoInputPartition(
                1,
                toBsonDocuments(
                    "{\"$addFields\": {\"__idx\": {\"0\": \"$a.b\", \"1\": \"$c\"}}}",
                    "{\"$match\": {\"__idx\": { \"$gte\": {\"0\": 250, \"1\": \"z\"}, \"$lt\": {\"0\": 500, \"1\": \"a\"}}}}",
                    "{\"$unset\": \"__idx\"}",
                    "{\"$match\": {\"b\": {\"$gte\": 99}}}"),
                preferredLocations),
            new MongoInputPartition(
                2,
                toBsonDocuments(
                    "{\"$addFields\": {\"__idx\": {\"0\": \"$a.b\", \"1\": \"$c\"}}}",
                    "{\"$match\": {\"__idx\": { \"$gte\": {\"0\": 500, \"1\": \"a\"}}}}",
                    "{\"$unset\": \"__idx\"}",
                    "{\"$match\": {\"b\": {\"$gte\": 99}}}"),
                preferredLocations)),
        createMongoInputPartitions(
            toBsonDocuments(
                "{\"_id\": {\"min\": {\"0\": 0, \"1\": \"a\"}, \"max\": {\"0\": 250, \"1\": \"z\"}}}",
                "{\"_id\": {\"min\": {\"0\": 250, \"1\": \"z\"}, \"max\": {\"0\": 500, \"1\": \"a\"}}}",
                "{\"_id\": {\"min\": {\"0\": 500, \"1\": \"a\"}, \"max\": {\"0\": 500, \"1\": \"z\"}}}"),
            usersPipeline,
            multipleFieldList,
            "__idx",
            preferredLocations));

    // Error scenarios
    assertThrows(
        IllegalArgumentException.class,
        () -> createMongoInputPartitions(
            emptyList(), emptyList(), singleFieldList, "a", emptyList()));
    assertThrows(
        IllegalArgumentException.class,
        () -> createMongoInputPartitions(
            toBsonDocuments("{\"_id\": {\"min\": 0, \"max\": 250}}"),
            emptyList(),
            singleFieldList,
            "a",
            emptyList()),
        "Single bounds");
    assertThrows(
        IllegalArgumentException.class,
        () -> createMongoInputPartitions(
            toBsonDocuments("{\"min\": 0, \"max\": 250}", "{\"min\": 250, \"max\": 500}"),
            emptyList(),
            singleFieldList,
            "a",
            emptyList()),
        "No _id field");
    assertThrows(
        IllegalArgumentException.class,
        () -> createMongoInputPartitions(
            toBsonDocuments("{\"_id\": {\"max\": 0}}", "{\"_id\": {\"min\": 250, \"max\": 500}}"),
            emptyList(),
            singleFieldList,
            "a",
            emptyList()),
        "Missing min field");
    assertThrows(
        IllegalArgumentException.class,
        () -> createMongoInputPartitions(
            toBsonDocuments("{\"_id\": {\"min\": 0}}", "{\"_id\": {\"min\": 250, \"max\": 500}}"),
            emptyList(),
            singleFieldList,
            "a",
            emptyList()),
        "Missing max field");
  }

  private void assertPipeline(final List<String> json, final List<BsonDocument> actual) {
    List<BsonDocument> expected = toBsonDocuments(json.toArray(new String[0]));
    assertIterableEquals(
        expected,
        actual,
        () -> "Got : "
            + actual.stream().map(BsonDocument::toJson).collect(Collectors.joining(",", "[", "]")));
  }

  private static List<BsonDocument> toBsonDocuments(final String... json) {
    return Arrays.stream(json).map(BsonDocument::parse).collect(Collectors.toList());
  }
}
