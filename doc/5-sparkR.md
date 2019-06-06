# Mongo Spark Connector SparkR

The following code snippets can be found in [introduction.R](../examples/src/test/r/tour/introduction.R).

## Prerequisites

Have MongoDB up and running and Spark 2.2.x downloaded. See the [introduction](0-introduction.md) and the [SQL](1-sparkSQL.md)
for more information on getting started.

You can run the interactive sparkR shell like so:

```
./bin/sparkR  --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/test.coll?readPreference=primaryPreferred" \
              --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/test.coll" \
              --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.3
```

## The Spark R API Basics

The Spark R API works via DataFrames and uses underlying Scala DataFrame.

## DataFrames and Datasets

Creating a dataframe is easy you can either load the data via `DefaultSource` ("mongo").

First, in an empty collection we load the following data:

```r
charactersRdf <- data.frame(list(name=c("Bilbo Baggins", "Gandalf", "Thorin", "Balin", "Kili", "Dwalin", "Oin", "Gloin", "Fili", "Bombur"),
                                 age=c(50, 1000, 195, 178, 77, 169, 167, 158, 82, NA)))
charactersSparkdf <- createDataFrame(sqlContext, charactersRdf)
write.df(charactersSparkdf, "", source = "mongo", mode = "overwrite")
```

Then to load the characters into a DataFrame via the standard source method:

```r
characters <- read.df(sqlContext, source = "mongo")
printSchema(characters)
```

Will return:

```
root
 |-- _id: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- name: string (nullable = true)
```

### SQL

Just like the Scala examples, SQL can be used to filter data. In the following example we register a temp table and then filter and output 
the characters with ages under 100:

```r
registerTempTable(characters, "characters")
centenarians <- sql(sqlContext, "SELECT name, age FROM characters WHERE age >= 100")
head(centenarians)
```

Outputs:

```
     name  age
1 Gandalf 1000
2  Thorin  195
3   Balin  178
4  Dwalin  169
5     Oin  167
6   Gloin  158
```

-----

[Next - FAQs](6-FAQ.md)
