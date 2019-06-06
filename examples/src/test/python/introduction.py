# -*- coding: UTF-8 -*-
#
# Copyright 2016 MongoDB, Inc.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# To run this example use:
# ./bin/spark-submit --master "local[4]"  \
#                    --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/test.coll?readPreference=primaryPreferred" \
#                    --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/test.coll" \
#                    --packages org.mongodb.spark:mongo-spark-connector_2.11:2.0.0 \
#                    introduction.py

from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession.builder.appName("Python Spark SQL basic example").getOrCreate()

    logger = spark._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.FATAL)

    # Save some data
    characters = spark.createDataFrame([("Bilbo Baggins",  50), ("Gandalf", 1000), ("Thorin", 195), ("Balin", 178), ("Kili", 77), ("Dwalin", 169), ("Oin", 167), ("Gloin", 158), ("Fili", 82), ("Bombur", None)], ["name", "age"])
    characters.write.format("mongo").mode("overwrite").save()

    # print the schema
    print("Schema:")
    characters.printSchema()

    # read from MongoDB collection
    df = spark.read.format("mongo").load()

    # SQL
    df.registerTempTable("temp")
    centenarians = spark.sql("SELECT name, age FROM temp WHERE age >= 100")
    print("Centenarians:")
    centenarians.show()
