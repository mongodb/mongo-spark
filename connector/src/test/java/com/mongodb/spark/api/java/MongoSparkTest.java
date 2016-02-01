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
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.catalyst.JavaTypeInference;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.junit.Assert.assertEquals;

public final class MongoSparkTest extends RequiresMongoDB {

    List<Document> counters = asList(Document.parse("{counter: 0}"), Document.parse("{counter: 1}"), Document.parse("{counter: 2}"));

    @Test
    public void shouldBeCreatableFromTheSparkContext() {
        JavaSparkContext sc = new JavaSparkContext(getSparkContext());
        MongoSpark.save(sc.parallelize(counters));
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
        MongoSpark.save(sc.parallelize(counters));
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

    @Test
    public void shouldBeAbleToCreateADataFrameByInferringTheSchema() {
        // Given
        JavaSparkContext sc = new JavaSparkContext(getSparkContext());
        MongoSpark.save(sc.parallelize(counters));
        JavaMongoRDD<Document> mongoRDD = MongoSpark.load(sc);

        StructField _idField = createStructField("_id", DataTypes.StringType, true);
        StructField countField = createStructField("counter", DataTypes.IntegerType, true);
        StructType expectedSchema = createStructType(asList(_idField, countField));

        // when
        DataFrame dataFrame = mongoRDD.toDF();

        // then
        assertEquals(dataFrame.schema(), expectedSchema);
        assertEquals(dataFrame.count(), 3);
    }

    @Test
    public void shouldBeAbleToCreateADataFrameUsingJavaBean() {
        // Given
        JavaSparkContext sc = new JavaSparkContext(getSparkContext());
        MongoSpark.save(sc.parallelize(counters));
        JavaMongoRDD<Document> mongoRDD = MongoSpark.load(sc);

        StructType expectedSchema = (StructType) JavaTypeInference.inferDataType(Counter.class)._1();

        // when
        DataFrame dataFrame = mongoRDD.toDF(Counter.class);

        // then
        assertEquals(dataFrame.schema(), expectedSchema);
        assertEquals(dataFrame.count(), 3);
    }

    @Test
    public void shouldBeAbleToCreateADatasetUsingJavaBean() {
        // Given
        JavaSparkContext sc = new JavaSparkContext(getSparkContext());
        MongoSpark.save(sc.parallelize(counters));
        JavaMongoRDD<Document> mongoRDD = MongoSpark.load(sc);

        StructType expectedSchema = (StructType) JavaTypeInference.inferDataType(Counter.class)._1();

        // when
        Dataset<Counter> dataset = mongoRDD.toDS(Counter.class);

        // then
        assertEquals(dataset.schema(), expectedSchema);
        assertEquals(dataset.count(), 3);
        assertEquals(dataset.map(new MapFunction<Counter, Integer>(){
            @Override
            public Integer call(final Counter counter) throws Exception {
                return counter.getCounter();
            }
        }, Encoders.INT()).collectAsList(), asList(0, 1, 2));
    }

    @Test(expected=SparkException.class)
    public void shouldThrowWhenCreatingADatasetWithInvalidData() {
        // Given
        JavaSparkContext sc = new JavaSparkContext(getSparkContext());
        MongoSpark.save(sc.parallelize(asList(Document.parse("{counter: 'a'}"), Document.parse("{counter: 'b'}"))));
        JavaMongoRDD<Document> mongoRDD = MongoSpark.load(sc);

        // when
        Dataset<Counter> dataset = mongoRDD.toDS(Counter.class);

        // then
        List<Integer> test = dataset.map(new MapFunction<Counter, Integer>() {
            @Override
            public Integer call(final Counter counter) throws Exception {
                return counter.getCounter();
            }
        }, Encoders.INT()).collectAsList();
    }

    @Test
    public void useDefaultValuesWhenCreatingADatasetWithMissingData() {
        // Given
        JavaSparkContext sc = new JavaSparkContext(getSparkContext());
        MongoSpark.save(sc.parallelize(asList(Document.parse("{name: 'a'}"), Document.parse("{name: 'b'}"))));
        JavaMongoRDD<Document> mongoRDD = MongoSpark.load(sc);

        // when
        Dataset<Counter> dataset = mongoRDD.toDS(Counter.class);

        // then - default values
        assertEquals(dataset.map(new MapFunction<Counter, Integer>(){
            @Override
            public Integer call(final Counter counter) throws Exception {
                return counter.getCounter();
            }
        }, Encoders.INT()).collectAsList(), asList(null, null));
    }

}
