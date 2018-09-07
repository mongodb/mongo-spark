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

package com.mongodb.spark;

import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.mongodb.spark.sql.CharacterBean;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public final class NoSparkConfTest extends JavaRequiresMongoDB {
    private final SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("MongoSparkConnector");

    @Test
    public void shouldBeAbleToUseConfigsWithRDDs() {
        WriteConfig writeConfig = WriteConfig.create(getOptions());
        ReadConfig readConfig = ReadConfig.create(getOptions());

        JavaSparkContext jsc = getJavaSparkContext(sparkConf);
        List<Document> documents = asList(Document.parse("{test: 0}"), Document.parse("{test: 1}"), Document.parse("{test: 2}"));

        MongoSpark.save(jsc.parallelize(documents), writeConfig);
        JavaMongoRDD<Document> mongoRDD = MongoSpark.load(jsc, readConfig);

        assertEquals(mongoRDD.count(), 3);

        List<Integer> counters = mongoRDD.map(new Function<Document, Integer>() {
            @Override
            public Integer call(final Document x) throws Exception {
                return x.getInteger("test");
            }
        }).collect();
        assertEquals(counters, asList(0,1,2));
    }

    @Test
    public void shouldBeAbleToUseConfigsWithDataFrames() {
        JavaSparkContext jsc = getJavaSparkContext(sparkConf);
        SparkSession sparkSession = SparkSession.builder().getOrCreate();
        List<CharacterBean> characters = asList(new CharacterBean("Gandalf", 1000), new CharacterBean("Bilbo Baggins", 50));

        MongoSpark.write(sparkSession.createDataFrame(jsc.parallelize(characters), CharacterBean.class))
                .options(getOptions())
                .save();

        List<CharacterBean> ds = MongoSpark.read(sparkSession)
                .options(getOptions())
                .load()
                .as(Encoders.bean(CharacterBean.class))
                .collectAsList();

        assertThat(ds, is(characters));
    }

    private Map<String, String> getOptions() {
        Map<String, String> options = new HashMap<String, String>();
        options.put("uri", getMongoClientURI());
        options.put("database", getDatabaseName());
        options.put("collection", getCollectionName());
        options.put("partitioner", "TestPartitioner$");
        return options;
    }
}
