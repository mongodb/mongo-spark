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

package com.mongodb.spark.sql;

import org.apache.spark.sql.catalyst.JavaTypeInference;
import org.apache.spark.sql.types.StructType;

/**
 * A helper for inferring the schema from Java
 *
 * In Spark 2.2.0 calling this method from Scala 2.10 caused compilation errors with the shadowed library in
 * `JavaTypeInference`. Moving it into Java stops Scala falling over and allows it to continue to work.
 *
 * See: SPARK-126
 */
final class MongoInferSchemaJava {

    @SuppressWarnings("unchecked")
    public static <T> StructType reflectSchema(final Class<T> beanClass) {
        return (StructType) JavaTypeInference.inferDataType(beanClass)._1();
    }

}
