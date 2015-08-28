+++
date = "2015-08-28T14:12:16-04:00"
draft = true
title = "Schema Inference"
[menu.main]
  parent = "SQL Reference"
  identifier = "SQL Schema Inference"
  weight = 70
  pre = "<i class='fa'></i>"
+++

## Schema Inference

For a walkthrough of the Spark SQL functionality please refer to the
[Quick Tour]({{< relref "sql/getting-started/quick-tour.md" >}}).

The `SchemaProvider` class provides two methods of obtaining a Spark SQL schema
for a MongoDB collection. Both methods analyze the structure of one or more
documents and map the field value types to their corresponding Spark SQL
[`DataType`](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.types.DataType).

### getSchemaFromDocument

This method generates a Spark SQL schema from the structure of a single
document. It is recommended to use this method if the structure of documents in
the collection is known.

```java
StructType schema = SchemaProvider.getSchemaFromDocument(document);
```

For example, a collection containing documents of the structure:

```javascript
{
    a: 1.0,
    b: [ "hello", "world" ]
}
```

would map to the following Spark SQL schema:

```
root
 |-- a: double (nullable = true)
 |-- b: array (nullable = true)
 |    |-- element: string (containsNull = true)
```

### getSchema

{{% note class="important" %}}
This method utilizes the `$sample` aggregation operator, which is available in
server versions 3.2 and above.
{{% /note %}}

This method generates a Spark SQL schema from a sample of documents in a MongoDB
collection. All fields and values are analyzed in order to ensure compatibility
of `DataType`s.

```java
StructType schema = SchemaProvider.getSchema(collectionProvider, sampleSize);
```

#### Conflicting Types

Suppose there are two documents from which the schema is inferred:
```javascript
{
    "a": 1.0,
    "b": [ 1.0 ]
}

{
    "a": 1.0,
    "b": "string"
}
```

The corresponding Spark SQL schemas for each document take the form:

```
root
 |-- a: double (nullable = true)
 |-- b: array (nullable = true)
 |    |-- element: integer (containsNull = true)

root
 |-- a: double (nullable = true)
 |-- b: string (nullable = true)
```

Notice that the field `"b"` has conflicting `DataType`s. During schema
inference for a collection, the values of fields are checked to ensure that the
ultimate `DataType` for the field in the result schema is compatible. If a
conflict of incompatible types occurs for a given field, then that field is
marked as having a `ConflictType`. In this example, the schema inferred for the
collection takes the form:

```
root
 |-- a: double (nullable = true)
 |-- b: conflict (nullable = true)
```
