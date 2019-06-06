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
#                    introduction.R

# Load SparkR library into your R session
library(SparkR)

# Initialize SparkSession
sparkR.session()
  
# Save some data
charactersRdf <- data.frame(list(name=c("Bilbo Baggins", "Gandalf", "Thorin", "Balin", "Kili", "Dwalin", "Oin", "Gloin", "Fili", "Bombur"),
                                 age=c(50, 1000, 195, 178, 77, 169, 167, 158, 82, NA)))
charactersSparkdf <- createDataFrame(charactersRdf)
write.df(charactersSparkdf, "", source = "mongo", mode = "overwrite")

# Load the data
characters <- read.df("", source = "mongo")
print("Schema:")
printSchema(characters)

# SQL
createOrReplaceTempView(characters, "characters")
centenarians <- sql("SELECT name, age FROM characters WHERE age >= 100")
print("Centenarians:")
head(centenarians)
