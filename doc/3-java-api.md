# Mongo Spark Connector Java API

The following code snippets can be found in [JavaIntroduction.java](../examples/src/test/java/tour/JavaIntroduction.java).

## Prerequisites

Have MongoDB up and running and Spark 2.2.x downloaded. See the [introduction](0-introduction.md) for more information on getting started.

## The Java API Basics

The main Java API feeds off the `com.mongodb.spark.MongoSpark` helper that provides an easy way to interact with MongoDB.
The configuration classes also have Java friendly `create` methods that should be preferred over using the native scala `apply` methods.

### Saving RDD data

Following the Scala introduction examples we'll walk through saving and loading data from MongoDB using Java and the Java friendly API's.

*Note:* When saving `RDD` data into MongoDB, it must be a type that can be converted into a Bson document.
You may have add a `map` step to transform the data into a `Document` (or `BsonDocument` a `DBObject`).

Add Documents to the collection using a `JavaSparkContext`:

```java
import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import static java.util.Arrays.asList;

JavaSparkContext jsc; // Create a Java Spark Context

JavaRDD<Document> documents = jsc.parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map
        (new Function<Integer, Document>() {
    @Override
    public Document call(final Integer i) throws Exception {
        return Document.parse("{test: " + i + "}");
    }
});

MongoSpark.save(documents);
```

To change which collection the data is inserted into or how the data is inserted, also supply a `WriteConfig` to the `MongoSpark#save` method.
The following example saves data to the "spark" collection, with a `majority` WriteConcern:

```java
import com.mongodb.spark.config.WriteConfig;

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
```

### Loading data as RDDs

You can pass a `JavaSparkContext` or a `SQLContext` to the `MongoSpark#load` for easy reading from MongoDB into an `JavaRDD`. The following
example loads the data we previously saved into the "coll" collection in the "test" database.

```java
// Loading and analyzing data from MongoDB
JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
System.out.println(rdd.count());
System.out.println(rdd.first().toJson());
```

To change where the data is read from or how the data is read, supply a `ReadConfig` to the `MongoSpark#load` method.
The following example reads from the "spark" collection, with a `secondaryPreferred` ReadPreference:

```java
// Loading data with a custom ReadConfig
Map<String, String> readOverrides = new HashMap<String, String>();
readOverrides.put("collection", "spark");
readOverrides.put("readPreference.name", "secondaryPreferred");
ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

JavaMongoRDD<Document> customRdd = MongoSpark.load(jsc, readConfig);

System.out.println(customRdd.count());
System.out.println(customRdd.first().toJson());
```

Where possible filter data in MongoDB, so less data has to be passed over the wire into Spark.  A `JavaMongoRDD` instance can be
passed an [aggregation pipeline](https://docs.mongodb.org/manual/core/aggregation-pipeline/) allowing data to be filtered from
MongoDB before its passed to Spark.

The following example also filters all documents where the "test" field has a value greater than 5 but only those matching documents are
passed to Spark.

```java
// Filtering an rdd using an aggregation pipeline before passing data to Spark
JavaMongoRDD<Document> aggregatedRdd = rdd.withPipeline(singletonList(Document.parse("{ $match: { test : { $gt : 5 } } }")));
System.out.println(aggregatedRdd.count());
System.out.println(aggregatedRdd.first().toJson());
```

## Datasets

Creating a Dataset is easy you can either load the data via `DefaultSource` or use the `JavaMongoRDD#toDF` method.

*Note:* In Spark 2.0 the `DataFrame` class became a type alias to `DataSet<Row>`. Java code must be updated to explicitly use `DataSet<Row>`.

First, in an empty collection we load the following data:

```java
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
```

Then to load the characters into a DataFrame via the standard source method:

```java
Dataset<Row> df = MongoSpark.load(jsc).toDF();
df.printSchema();
```

Will return:

```
root
 |-- _id: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- name: string (nullable = true)
```


By default reading from MongoDB in a `SparkSession` infers the schema by sampling documents from the database.
If you know the shape of your documents then you can use a simple java bean to define the schema instead, thus preventing the extra queries.

In the following example we define a `Character` Java bean and pass that to the `toDF` method:

```java
SparkSession sparkSession = SparkSession.builder().getOrCreate();
Dataset<Row> explicitDF = MongoSpark.load(sparkSession).toDF(Character.class);
explicitDF.printSchema();
```
Will return:

```
root
 |-- age: integer (nullable = true)
 |-- name: string (nullable = true)
```

-----
*Note:* Use the `toDS` method to convert a `JavaMongoRDD` into a `Dataset`.

-----

### SQL

Just like the Scala examples, SQL can be used to filter data. In the following example we register a temp table and then filter and output
the characters with ages under 100:

```java
explicitDF.registerTempTable("characters");
Dataset<Row> centenarians = sparkSession.sql("SELECT name, age FROM characters WHERE age >= 100");
```

### Saving Datasets

The connector provides the ability to persist data into MongoDB.

In the following example we save the centenarians into the "hundredClub" collection:

```scala
MongoSpark.write(centenarians).option("collection", "hundredClub").save();

// Load the data from the "hundredClub" collection
MongoSpark.load(sparkSession, ReadConfig.create(sqlContext).withOption("collection", "hundredClub"), Character.class).show();
```

Outputs:

```
+-------+----+
|   name| age|
+-------+----+
|Gandalf|1000|
| Thorin| 195|
|  Balin| 178|
| Dwalin| 169|
|    Óin| 167|
|  Glóin| 158|
+-------+----+
```

### Defining and filtering unsupported bson data types

As not all Bson types have equivalent Spark types querying them outside of using Datasets can be verbose and requires in depth knowledge of
the `StructType` that can be used to represent those types. See the [sparkSQL](3-sparkSQL#defining-and-filtering-unsupported-bson-data-types)
section for more information.

Below is an example of using the helpers when defining and querying an `ObjectId` field:

```java
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

```

Outputs:

```
+--------------------+---+
|                 _id|  a|
+--------------------+---+
|[1234000000000000...|  1|
+--------------------+---+
```


-----

[Next - pySpark](4-pyspark.md)
