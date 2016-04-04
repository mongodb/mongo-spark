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
#                    --packages org.mongodb.spark:mongo-spark-connector_2.10:0.1 \
#                    introduction.py

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext


if __name__ == "__main__":

    sparkConf = SparkConf().setMaster("local").setAppName("MongoSparkConnectorTour").set("spark.app.id", "MongoSparkConnectorTour")
    sc = SparkContext(conf=sparkConf)
    sqlContext = SQLContext(sc)

    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.FATAL)

    # Save some data
    charactersRdd = sc.parallelize([("Bilbo Baggins",  50), ("Gandalf", 1000), ("Thorin", 195), ("Balin", 178), ("Kili", 77),
                                    ("Dwalin", 169), ("Oin", 167), ("Gloin", 158), ("Fili", 82), ("Bombur", None)])
    characters = sqlContext.createDataFrame(charactersRdd, ["name", "age"])
    characters.write.format("com.mongodb.spark.sql").mode("overwrite").save()

    # Load the data
    df = sqlContext.read.format("com.mongodb.spark.sql").load()
    print("Schema:")
    df.printSchema()

    # SQL
    df.registerTempTable("characters")
    centenarians = sqlContext.sql("SELECT name, age FROM characters WHERE age >= 100")
    print("Centenarians:")
    centenarians.show()

    sc.stop()
