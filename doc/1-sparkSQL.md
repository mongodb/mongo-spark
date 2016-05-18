# Mongo Spark Connector Spark SQL

The following code snippets can be found in [SparkSQL.scala](../examples/src/test/scala/tour/SparkSQL.scala).

## Prerequisites

Have MongoDB up and running and the Spark 1.6.x downloaded. This tutorial will use the Spark Shell allowing for instant feedback.
See the [introduction](0-introduction.md) for more information.

Insert some sample data into an empty database:

```scala
import org.bson.Document
import com.mongodb.spark._

val docs = """
 |{"name": "Bilbo Baggins", "age": 50}
 |{"name": "Gandalf", "age": 1000}
 |{"name": "Thorin", "age": 195}
 |{"name": "Balin", "age": 178}
 |{"name": "Kíli", "age": 77}
 |{"name": "Dwalin", "age": 169}
 |{"name": "Óin", "age": 167}
 |{"name": "Glóin", "age": 158}
 |{"name": "Fíli", "age": 82}
 |{"name": "Bombur"}""".trim.stripMargin.split("[\\r\\n]+").toSeq
sc.parallelize(docs.map(Document.parse)).saveToMongoDB()
```

## Spark SQL

The entry point to Spark SQL is the `SQLContext` class or one of its descendants. To create a basic `SQLContext` all you need is a
`SparkContext`.

```scala
import org.apache.spark.sql.SQLContext

val sc: SparkContext // An existing SparkContext.
val sqlContext = new SQLContext(sc)
```

First enable the Mongo Connector specific functions on the `SQLContext`:

```scala
import com.mongodb.spark.sql._
```

### DataFrames and DataSets

Creating a DataFrame is easy using the implicit `mongo` helper on the `DataFrameReader`:

```scala
val df = sqlContext.read.mongo()
df.printSchema()
```

Will return:

```
root
 |-- _id: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- name: string (nullable = true)
```

-----
*Note:* The `sqlContext.read.mongo()` implicit function is the equivalent of `sqlContext.read.format("com.mongodb.spark.sql").load()`

-----

In the following example we can filter and output the characters with ages under 100:

```scala
df.filter(df("age") < 100).show()
```

```
+--------------------+---+-------------+
|                 _id|age|         name|
+--------------------+---+-------------+
|56eaa0d7ba58b9043...| 50|Bilbo Baggins|
|56eaa0d7ba58b9043...| 77|         Kíli|
|56eaa0d7ba58b9043...| 82|         Fíli|
+--------------------+---+-------------+
```

-----
*Note:* Unlike RDD's when using `filters` with DataFrames or SparkSQL the underlying Mongo Connector code will construct an aggregation pipeline to filter the data in MongoDB before sending it to Spark.

-----

#### Schema inference and explicitly declaring a schema

By default reading from MongoDB in a `SQLContext` infers the schema by sampling documents from the database. 
If you know the shape of your documents then you can use a simple case class to define the schema instead, thus preventing the extra queries.

-----
**Note:** When providing a case class for the schema *only the declared fields* will be returned by MongoDB, helping minimize the data sent across the wire.

-----

The following example creates Character case class and then uses it to represent the documents / schema for the collection:

```scala
case class Character(name: String, age: Int)
val explicitDF = sqlContext.read.mongo[Character]()
explicitDF.printSchema()
```

Outputs:
```
root
 |-- name: string (nullable = true)
 |-- age: integer (nullable = false)

```

The following example converts the `DataFrame` into a `DataSet`:

```scala
explicitDF.as[Character]
```

#### RDD to DataFrame / DataSets

The `MongoRDD` class provides helpers to create DataFrames and DataSets directly:

```scala
val dataframeInferred = sqlContext.loadFromMongoDB().toDF()
val dataframeExplicit = sqlContext.loadFromMongoDB().toDF[Character]()
val dataset = sqlContext.loadFromMongoDB().toDS[Character]()
```

### SQL queries

Spark SQL works on top of DataFrames, to be able to use SQL you need to register a temporary table first and then you can run SQL queries
over the data.

The following example registers a "characters" table and then queries it to find all characters that are 100 or older.

```scala
val characters = sqlContext.loadFromMongoDB().toDF[Character]()
characters.registerTempTable("characters")

val centenarians = sqlContext.sql("SELECT name, age FROM characters WHERE age >= 100")
centenarians.show()
```

-----
**Note:** You must use the same `SQLContext` that registers the table when querying it. 

-----

### Saving DataFrames

The connector provides the ability to persist data into MongoDB.

In the following example we save the centenarians into the "hundredClub" collection:

```scala
centenarians.write.option("collection", "hundredClub").mongo()
println("Reading from the 'hundredClub' collection:")
sqlContext.read.option("collection", "hundredClub").mongo[Character]().show()
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

-----
**Note:** The `centenarians.write.mongo()` implicit function is the equivalent of `centenarians.write.format("com.mongodb.spark.sql").save()`

-----

## DataTypes

Spark supports a limited number of data types, to ensure that all bson types can be round tripped in and out of Spark DataFrames / 
DataSets. Custom StructTypes are created for any unsupported Bson Types. The following table shows the mapping between the Bson Types and 
Spark Types:

Bson Type               | Spark Type
------------------------|---------------------------------------------------------------------
`Document`              | `StructType`
`Array`                 | `ArrayType`
`32-bit integer`        | `Integer`
`64-bit integer`        | `Long`
`Binary data`           | `Array[Byte]` or `StructType`: `{ subType: Byte, data: Array[Byte]}`
`Boolean`               | `Boolean`
`Date`                  | `java.sql.Timestamp`
`DBPointer`             | `StructType`: `{ ref: String , oid: String}`
`Double`                | `Double`
`JavaScript`            | `StructType`: `{ code: String }`
`JavaScript with scope` | `StructType`: `{ code: String , scope: String }`
`Max key`               | `StructType`: `{ maxKey: Integer }`
`Min key`               | `StructType`: `{ minKey: Integer }`
`Null`                  | `null`
`ObjectId`              | `StructType`: `{ oid: String }`
`Regular Expression`    | `StructType`: `{ regex: String , options: String }`
`String`                | `String`
`Symbol`                | `StructType`: `{ symbol: String }`
`Timestamp`             | `StructType`: `{ time: Integer , inc: Integer }`
`Undefined`             | `StructType`: `{ undefined: Boolean }`

### Dataset support

To help better support DataSets, the following Scala case classes and JavaBean classes have been created to represent the unsupported Bson 
Types:

Bson Type               | Scala case class                       | JavaBean
------------------------|----------------------------------------|----------------------------------------------
                        | `com.mongodb.spark.sql.fieldTypes`     | `com.mongodb.spark.api.java.sql.fieldTypes.`
`Binary data`           | `Binary`                               | `Binary`
`DBPointer`             | `DBPointer`                            | `DBPointer`
`JavaScript`            | `JavaScript`                           | `JavaScript`
`JavaScript with scope` | `JavaScriptWithScope`                  | `JavaScriptWithScope`
`Max key`               | `MaxKey`                               | `MaxKey`
`Min key`               | `MinKey`                               | `MinKey`
`ObjectId`              | `ObjectId`                             | `ObjectId`
`Regular Expression`    | `RegularExpression`                    | `RegularExpression`
`Symbol`                | `Symbol`                               | `Symbol`
`Timestamp`             | `Timestamp`                            | `Timestamp`
`Undefined`             | `Undefined`                            | `Undefined`

For convenience all Bson Types can be represented as a String value as well, however these values lose all their type information and if 
saved back to MongoDB they would be stored as a String.


[Next - Configuring](2-configuring.md)
