+++
date = "2015-08-25T11:13:10-04:00"
draft = true
title = "Quick Tour"
[menu.main]
  parent = "SQL Getting Started"
  identifier = "SQL Quick Tour"
  weight = 10
  pre = "<i class='fa'></i>"
+++

# Mongo Spark SQL Connector Quick Tour

The following code snippets come from the `SQLQuickTour.java` example code that
can be found with the [connector source]({{< srcref "examples/tour/src/main/java/com/mongodb/spark/examples/tour/SQLQuickTour.java" >}}).

{{% note %}}
See the [installation guide]({{< relref "sql/getting-started/installation-guide.md" >}})
for instructions on how to install the Mongo Spark connector.
{{% /note %}}

{{% note %}}
See the [core connector quick tour]({{< relref "core/getting-started/quick-tour.md" >}})
for instructions on how to utilize the core Mongo Spark connector.
{{% /note %}}

## Structuring the Unstructured

In order to execute Spark SQL queries against data from MongoDB, the collection
must be mapped to a structured form.

### Schema Inference

First, a schema representing the structure of documents in a given collection
may be generated.

```java
StructType schema = SchemaProvider.getSchema(collectionProvider, 1000);
```

Alternatively, a schema may be generated from a document possessing the structure
of documents in the collection. This method may prove useful for specifying
only particular fields needed for analysis.

```java
Document document = new Document("a", 1.0);
StructType documentSchema = SchemaProvider.getSchemaFromDocument(document);
```

### Getting a Mongo Collection in Row Format

A RDD of Documents may be converted to an RDD of Spark SQL Rows using the
generated schema.

```java
JavaRDD<Row> rdd = msc.parallelize(Document.class, collectionProvider, splitKey)
                      .map(doc -> DocumentRowConverter.documentToRow(doc, schema));
```

## Creating a DataFrame

In order to execute queries on the table rows, a
[`DataFrame`](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrame)
must be created and the table must be registered with the
[`SQLContext`](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.SQLContext).

```java
DataFrame frame = sqlContext.createDataFrame(rdd, schema);
frame.registerTempTable("test");
```

## Querying the Table

At this point, the table has been registered with the `SQLContext`, and may now
be queried.

```java
List<Row> results = sqlContext.sql("SELECT * FROM test").collectAsList();
```
