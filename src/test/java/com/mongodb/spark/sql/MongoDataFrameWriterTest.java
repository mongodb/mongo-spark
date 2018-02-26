package com.mongodb.spark.sql;

import com.mongodb.spark.JavaRequiresMongoDB;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public final class MongoDataFrameWriterTest extends JavaRequiresMongoDB {

    private final List<String> characters = asList(
            "{'name': 'Bilbo Baggins', 'age': 50}",
            "{'name': 'Gandalf', 'age': 1000}",
            "{'name': 'Thorin', 'age': 195}",
            "{'name': 'Balin', 'age': 178}",
            "{'name': 'Kíli', 'age': 77}",
            "{'name': 'Dwalin', 'age': 169}",
            "{'name': 'Óin', 'age': 167}",
            "{'name': 'Glóin', 'age': 158}",
            "{'name': 'Fíli', 'age': 82}"
    );

    @Test
    public void shouldBeEasilyCreatedFromMongoSpark() {
        // Given
        JavaSparkContext jsc = getJavaSparkContext();
        JavaRDD<CharacterBean> people = jsc.parallelize(characters).map(JsonToCharacter);
        SparkSession sparkSession = SparkSession.builder().getOrCreate();

        // When
        MongoSpark.write(sparkSession.createDataFrame(people, CharacterBean.class)).save();

        // Then
        assertEquals(MongoSpark.read(sparkSession).load().count(), 9);
    }

    @Test
    public void shouldBeEasilyCreatedFromADataFrameAndSaveToMongo() {
        // Given
        JavaSparkContext jsc = getJavaSparkContext();
        JavaRDD<CharacterBean> people = jsc.parallelize(characters).map(JsonToCharacter);
        SparkSession sparkSession = SparkSession.builder().getOrCreate();

        // When
        MongoSpark.write(sparkSession.createDataFrame(people, CharacterBean.class)).save();

        // Then
        assertEquals(MongoSpark.read(sparkSession).load().count(), 9);
    }


    @Test
    public void shouldTakeACustomOptions() {
        // Given
        String saveToCollectionName = getCollectionName() + "_new";
        Map<String, String> options = new HashMap<String, String>();
        options.put("collection", saveToCollectionName);
        JavaSparkContext jsc = getJavaSparkContext();
        WriteConfig writeConfig = WriteConfig.create(jsc.getConf()).withOptions(options);
        ReadConfig readConfig = ReadConfig.create(jsc.getConf()).withOptions(options);

        JavaRDD<CharacterBean> people = jsc.parallelize(characters).map(JsonToCharacter);
        SparkSession sparkSession = SparkSession.builder().getOrCreate();

        // When
        MongoSpark.write(sparkSession.createDataFrame(people, CharacterBean.class)).options(writeConfig.asOptions()).save();

        // Then
        assertEquals(MongoSpark.read(sparkSession).options(readConfig.asOptions()).load().count(), 9);
    }

    private static Function<String, CharacterBean> JsonToCharacter = new Function<String, CharacterBean>() {
        @Override
        public CharacterBean call(final String json) throws Exception {
            Document doc = Document.parse(json);
            CharacterBean character = new CharacterBean();
            character.setName(doc.getString("name"));
            character.setAge(doc.getInteger("age"));
            return character;
        }
    };

}

