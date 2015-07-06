/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.mongodb.spark;

import org.apache.spark.api.java.JavaRDD;

import org.bson.Document;

import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

/**
 * An extension of [[org.apache.spark.api.java.JavaRDD]] that wraps a MongoRDD.
 *
 * @param <TDocument> document type parameter
 */
public class MongoJavaRDD<TDocument> extends JavaRDD {
    private MongoRDD<TDocument> rdd;
    private Class               clazz;

    /**
     * Constructs a new instance.
     *
     * @param rdd the rdd
     * @param clazz [[java.lang.Class]] of the documents in the RDD
     */
    public MongoJavaRDD(final MongoRDD<TDocument> rdd, final Class clazz) {
        super(rdd, ClassTag$.MODULE$.apply(clazz));
        this.rdd = rdd;
        this.clazz = clazz;
    }

    /**
     * Constructs a new instance. Defaults the [[java.lang.Class]] of
     * the documents in the RDD to Document.class.
     *
     * @param rdd the rdd
     */
    public MongoJavaRDD(final MongoRDD<TDocument> rdd) {
        this(rdd, Document.class);
    }

    @Override
    public MongoRDD<TDocument> rdd() {
        return this.rdd;
    }

    @Override
    public ClassTag classTag() {
        return ClassTag$.MODULE$.apply(this.clazz);
    }
}
