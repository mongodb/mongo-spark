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

package tour;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.MongoConnector;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.mongodb.spark.sql.helpers.StructFields;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public final class JavaIntroduction {

    /**
     * Run this main method to see the output of this quick example.
     *
     * @param args takes an optional single argument for the connection string
     * @throws InterruptedException if a latch is interrupted
     */
    public static void main(final String[] args) throws InterruptedException {
        JavaSparkContext jsc = createJavaSparkContext(args);

        // Create a RDD
        JavaRDD<Document> documents = jsc.parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map
                (new Function<Integer, Document>() {
            @Override
            public Document call(final Integer i) throws Exception {
                return Document.parse("{test: " + i + "}");
            }
        });

        // Saving data from an RDD to MongoDB
        MongoSpark.save(documents);

        // Saving data with a custom WriteConfig
        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("collection", "spark");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);

        JavaRDD<Document> sparkDocuments = jsc.parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map
                (new Function<Integer, Document>() {
                    @Override
                    public Document call(final Integer i) throws Exception {
                        return Document.parse("{spark: " + i + "}");
                    }
                });
        // Saving data from an RDD to MongoDB
        MongoSpark.save(sparkDocuments, writeConfig);

        // Loading and analyzing data from MongoDB
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        System.out.println(rdd.count());
        System.out.println(rdd.first().toJson());

        // Loading data with a custom ReadConfig
        Map<String, String> readOverrides = new HashMap<String, String>();
        readOverrides.put("collection", "spark");
        readOverrides.put("readPreference.name", "secondaryPreferred");
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

        JavaMongoRDD<Document> customRdd = MongoSpark.load(jsc, readConfig);

        System.out.println(customRdd.count());
        System.out.println(customRdd.first().toJson());

        // Filtering an rdd using an aggregation pipeline before passing data to Spark
        JavaMongoRDD<Document> aggregatedRdd = rdd.withPipeline(singletonList(Document.parse("{ $match: { test : { $gt : 5 } } }")));
        System.out.println(aggregatedRdd.count());
        System.out.println(aggregatedRdd.first().toJson());

        // Datasets

        // Drop database
        dropDatabase(getMongoClientURI(args));

        // Add Sample Data
        List<String> characters = asList(
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
        MongoSpark.save(jsc.parallelize(characters).map(new Function<String, Document>() {
            @Override
            public Document call(final String json) throws Exception {
                return Document.parse(json);
            }
        }));


        // Load inferring schema
        Dataset<Row> df = MongoSpark.load(jsc).toDF();
        df.printSchema();
        df.show();

        // Declare the Schema via a Java Bean
        SparkSession sparkSession = SparkSession.builder().getOrCreate();
        Dataset<Row> explicitDF = MongoSpark.load(jsc).toDF(Character.class);
        explicitDF.printSchema();

        // SQL
        explicitDF.registerTempTable("characters");
        Dataset<Row> centenarians = sparkSession.sql("SELECT name, age FROM characters WHERE age >= 100");

        // Saving DataFrame
        MongoSpark.write(centenarians).option("collection", "hundredClub").save();
        MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("collection", "hundredClub"), Character.class).show();

        // Drop database
        MongoConnector.apply(jsc.sc()).withDatabaseDo(ReadConfig.create(sparkSession), new Function<MongoDatabase, Void>() {
            @Override
            public Void call(final MongoDatabase db) throws Exception {
                db.drop();
                return null;
            }
        });

        String objectId = "123400000000000000000000";
        List<Document> docs = asList(
                new Document("_id", new ObjectId(objectId)).append("a", 1),
                new Document("_id", new ObjectId()).append("a", 2));
        MongoSpark.save(jsc.parallelize(docs));

        // Set the schema using the ObjectId helper
        StructType schema = DataTypes.createStructType(asList(
                StructFields.objectId("_id", false),
                DataTypes.createStructField("a", DataTypes.IntegerType, false)));

        // Create a dataframe with the helper functions registered
        df = MongoSpark.read(sparkSession).schema(schema).option("registerSQLHelperFunctions", "true").load();

        // Query using the ObjectId string
        df.filter(format("_id = ObjectId('%s')", objectId)).show();
    }

    private static JavaSparkContext createJavaSparkContext(final String[] args) {
        String uri = getMongoClientURI(args);
        dropDatabase(uri);
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("MongoSparkConnectorTour")
                .set("spark.app.id", "MongoSparkConnectorTour")
                .set("spark.mongodb.input.uri", uri)
                .set("spark.mongodb.output.uri", uri);

        return new JavaSparkContext(conf);
    }

    private static String getMongoClientURI(final String[] args) {
        String uri;
        if (args.length == 0) {
            uri = "mongodb://localhost/test.coll"; // default
        } else {
            uri = args[0];
        }
        return uri;
    }

    private static void dropDatabase(final String connectionString) {
        MongoClientURI uri = new MongoClientURI(connectionString);
        new MongoClient(uri).dropDatabase(uri.getDatabase());
    }
}
