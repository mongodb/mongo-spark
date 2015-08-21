/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

package com.mongodb.spark;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * A dummy interface to provide a friendlier way to instantiate a lambda as
 * a serializable supplier by specifying the reference type and additional bound.
 *
 * See <a href="http://docs.oracle.com/javase/specs/jls/se8/html/jls-15.html#jls-15.16">Cast Expressions</a>
 *
 * @param <T> the type of results supplied by this supplier
 */
@FunctionalInterface
public interface SerializableSupplier<T> extends Supplier<T>, Serializable {
}
