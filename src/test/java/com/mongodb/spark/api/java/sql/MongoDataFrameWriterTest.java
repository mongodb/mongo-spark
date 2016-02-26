package com.mongodb.spark.api.java.sql;

import com.mongodb.spark.api.java.RequiresMongoDB;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SQLContext;
import org.bson.Document;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public final class MongoDataFrameWriterTest extends RequiresMongoDB {

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
    public void shouldBeEasilyCreatedFromADataFrameAndSaveToMongo() {
        // Given
        JavaSparkContext sc = new JavaSparkContext(getSparkContext());
        JavaRDD<Character> people = sc.parallelize(characters).map(JsonToCharacter);
        SQLContext sqlContext = new SQLContext(sc);

        // When
        sqlContext.createDataFrame(people, Character.class).write().format("com.mongodb.spark.sql").save();

        // Then
        assertEquals(sqlContext.read().format("com.mongodb.spark.sql").load().count(), 9);
    }

    @Test
    public void shouldTakeACustomOptions() {
        // Given
        String saveToCollectionName = getCollectionName() + "_new";
        Map<String, String> options = new HashMap<>();
        options.put("collectionName", saveToCollectionName);
        JavaSparkContext sc = new JavaSparkContext(getSparkContext());
        WriteConfig writeConfig = WriteConfig.create(sc.getConf()).withJavaOptions(options);
        ReadConfig readConfig = ReadConfig.create(sc.getConf()).withJavaOptions(options);

        JavaRDD<Character> people = sc.parallelize(characters).map(JsonToCharacter);
        SQLContext sqlContext = new SQLContext(sc);

        // When
        sqlContext.createDataFrame(people, Character.class).write().format("com.mongodb.spark.sql").options(writeConfig.asJavaOptions()).save();

        // Then
        assertEquals(sqlContext.read().format("com.mongodb.spark.sql").options(readConfig.asJavaOptions()).load().count(), 9);
    }

    private static Function<String, Character> JsonToCharacter = new Function<String, Character>() {
        @Override
        public Character call(final String json) throws Exception {
            Document doc = Document.parse(json);
            Character character = new Character();
            character.setName(doc.getString("name"));
            character.setAge(doc.getInteger("age"));
            return character;
        }
    };

}

