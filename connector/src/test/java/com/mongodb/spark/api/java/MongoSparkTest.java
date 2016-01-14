/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.spark.api.java;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public final class MongoSparkTest extends RequiresMongoDB {

    @Test
    public void shouldBeCreatableFromTheSparkContext() {
        JavaSparkContext sc = new JavaSparkContext(getSparkContext());
        MongoSpark.save(sc.parallelize(asList(Document.parse("{counter: 0}"), Document.parse("{counter: 1}"),
                Document.parse("{counter: 2}"))));
        JavaMongoRDD<Document> mongoRDD = MongoSpark.load(sc);

        assertEquals(mongoRDD.count(), 3);

        List<Integer> counters = mongoRDD.map(new Function<Document, Integer>() {
            @Override
            public Integer call(final Document x) throws Exception {
                return x.getInteger("counter");
            }
        }).collect();
        assertEquals(counters, asList(0,1,2));
    }

    @Test
    public void shouldBeAbleToHandleNoneExistentCollections() {
        JavaMongoRDD<Document> mongoRDD = MongoSpark.load(new JavaSparkContext(getSparkContext()));
        assertEquals(mongoRDD.count(), 0);
    }

    @Test
    public void shouldBeAbleToQueryViaAPipeLine() {
        JavaSparkContext sc = new JavaSparkContext(getSparkContext());
        MongoSpark.save(sc.parallelize(asList(Document.parse("{counter: 0}"), Document.parse("{counter: 1}"),
                Document.parse("{counter: 2}"))));
        JavaMongoRDD<Document> mongoRDD = MongoSpark.load(sc);

        assertEquals(mongoRDD.withPipeline(singletonList(Document.parse("{$match: { counter: {$gt: 0}}}"))).count(), 2);
        assertEquals(mongoRDD.withPipeline(singletonList(BsonDocument.parse("{$match: { counter: {$gt: 0}}}"))).count(), 2);
        assertEquals(mongoRDD.withPipeline(singletonList(Aggregates.match(Filters.gt("counter", 0)))).count(), 2);
    }

    @Test
    public void shouldBeAbleToHandleDifferentCollectionTypes() {
        JavaSparkContext sc = new JavaSparkContext(getSparkContext());
        MongoSpark.save(sc.parallelize(asList(BsonDocument.parse("{counter: 0}"), BsonDocument.parse("{counter: 1}"),
                BsonDocument.parse("{counter: 2}"))), BsonDocument.class);
        JavaMongoRDD<BsonDocument> mongoRDD = MongoSpark.load(sc, BsonDocument.class);

        assertEquals(mongoRDD.count(), 3);
    }
}
