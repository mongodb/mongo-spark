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

package com.mongodb.spark.sql;

import com.mongodb.spark.JavaRequiresMongoDB;
import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.junit.Assert.assertEquals;

public final class MongoDataFrameReaderTest extends JavaRequiresMongoDB {

    private final List<String> characters = asList(
        "{'name': 'Bilbo Baggins', 'age': 50}",
        "{'name': 'Gandalf', 'age': 1000}",
        "{'name': 'Thorin', 'age': 195}",
        "{'name': 'Balin', 'age': 178}",
        "{'name': 'Kíli', 'age': 77}",
        "{'name': 'Dwalin', 'age': 169}",
        "{'name': 'Óin', 'age': 167}",
        "{'name': 'Glóin', 'age': 158}",
        "{'name': 'Fíli', 'age': 82}",
        "{'name': 'Bombur'}"
    );

    @Test
    public void shouldBeEasilyCreatedViaMongoSparkAndSQLContext() {
        // Given
        JavaSparkContext jsc = getJavaSparkContext();
        MongoSpark.save(jsc.parallelize(characters).map(JsonToDocument));
        StructType expectedSchema = createStructType(asList(_idField, ageField, nameField));

        // When
        Dataset<Row> df = MongoSpark.load(jsc).toDF();

        // Then
        assertEquals(df.schema(), expectedSchema);
        assertEquals(df.count(), 10);
        assertEquals(df.filter("age > 100").count(), 6);
    }

    @Test
    public void shouldBeEasilyCreatedViaMongoSpark() {
        // Given
        JavaSparkContext jsc = getJavaSparkContext();
        MongoSpark.save(jsc.parallelize(characters).map(JsonToDocument));
        StructType expectedSchema = createStructType(asList(_idField, ageField, nameField));

        // When
        Dataset<Row> df = MongoSpark.load(jsc).toDF();

        // Then
        assertEquals(df.schema(), expectedSchema);
        assertEquals(df.count(), 10);
        assertEquals(df.filter("age > 100").count(), 6);
    }

    @Test
    public void shouldBeEasilyCreatedFromTheSQLContext() {
        // Given
        JavaSparkContext jsc = getJavaSparkContext();
        MongoSpark.save(jsc.parallelize(characters).map(JsonToDocument));
        StructType expectedSchema = createStructType(asList(_idField, ageField, nameField));

        // When
        Dataset<Row> df = MongoSpark.load(jsc).toDF();

        // Then
        assertEquals(df.schema(), expectedSchema);
        assertEquals(df.count(), 10);
        assertEquals(df.filter("age > 100").count(), 6);
    }

    @Test
    public void shouldIncludeAnyPipelinesWhenInferringTheSchema() {
        // Given
        JavaSparkContext jsc = getJavaSparkContext();
        MongoSpark.save(jsc.parallelize(characters).map(JsonToDocument));
        MongoSpark.save(jsc.parallelize(asList("{counter: 1}", "{counter: 2}", "{counter: 3}")).map(JsonToDocument));

        SparkSession sparkSession = SparkSession.builder().getOrCreate();
        StructType expectedSchema = createStructType(asList(_idField, ageField, nameField));

        // When
        Dataset<Row> df = sparkSession.read().format("com.mongodb.spark.sql").option("pipeline", "{ $match: { name: { $exists: true } } }").load();

        // Then
        assertEquals(df.schema(), expectedSchema);
        assertEquals(df.count(), 10);
        assertEquals(df.filter("age > 100").count(), 6);

        // When - single item pipeline
        df = sparkSession.read().format("com.mongodb.spark.sql").option("pipeline", "{ $match: { name: { $exists: true } } }").load();

        // Then
        assertEquals(df.schema(), expectedSchema);
        assertEquals(df.count(), 10);
        assertEquals(df.filter("age > 100").count(), 6);
    }

    @Test
    public void shouldBeEasilyCreatedWithMongoSparkAndJavaBean() {
        // Given
        JavaSparkContext jsc = getJavaSparkContext();
        MongoSpark.save(jsc.parallelize(characters).map(JsonToDocument));
        StructType expectedSchema = createStructType(asList(ageField, nameField));

        // When
        Dataset<Row> df = MongoSpark.load(jsc).toDF(CharacterBean.class);

        // Then
        assertEquals(df.schema(), expectedSchema);
        assertEquals(df.count(), 10);
        assertEquals(df.filter("age > 100").count(), 6);
    }


    @Test
    public void shouldBeEasilyCreatedWithAProvidedRDDAndJavaBean() {
        // Given
        JavaSparkContext jsc = getJavaSparkContext();
        MongoSpark.save(jsc.parallelize(characters).map(JsonToDocument));
        StructType expectedSchema = createStructType(asList(ageField, nameField));

        // When
        Dataset<Row> df = MongoSpark.load(jsc).toDF(CharacterBean.class);

        // Then
        assertEquals(df.schema(), expectedSchema);
        assertEquals(df.count(), 10);
        assertEquals(df.filter("age > 100").count(), 6);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowAnExceptionIfPipelineIsInvalid() {
        JavaSparkContext jsc = getJavaSparkContext();
        MongoSpark.save(jsc.parallelize(characters).map(JsonToDocument));

        SparkSession.builder().getOrCreate().read().format("com.mongodb.spark.sql").option("pipeline", "[1, 2, 3]").load();
    }

    private StructField _idField = createStructField("_id", ObjectIdStruct(), true);
    private StructField nameField = createStructField("name", DataTypes.StringType, true);
    private StructField ageField = createStructField("age", DataTypes.IntegerType, true);

}
