/*
 * Copyright 2008-present MongoDB, Inc.
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
 * limitations under t1
 *
 */

package com.mongodb.spark.sql.connector.interop;

import java.util.List;
import java.util.Map;
import scala.collection.JavaConverters;

/** Utils object to convert Java To Scala to enable cross build */
@SuppressWarnings("deprecated")
public final class JavaScala {
  private JavaScala() {}

  /**
   * Wrapper to convert a java map to a scala map
   *
   * @param data java collection
   * @param <K> key
   * @param <V> value
   * @return scala collection
   */
  public static <K, V> scala.collection.Map<K, V> asScala(final Map<K, V> data) {
    return JavaConverters.mapAsScalaMap(data);
  }

  /**
   * Wrapper to convert a java list to a scala seq
   *
   * @param data java collection
   * @param <A> value
   * @return scala collection
   */
  public static <A> scala.collection.Seq<A> asScala(final List<A> data) {
    return JavaConverters.collectionAsScalaIterable(data).toSeq();
  }

  /**
   * Wrapper to convert a scala map to a java map
   *
   * @param data java collection
   * @param <K> key
   * @param <V> value
   * @return scala collection
   */
  public static <K, V> Map<K, V> asJava(final scala.collection.Map<K, V> data) {
    return JavaConverters.mapAsJavaMap(data);
  }

  /**
   * Wrapper to convert a scala seq to a java list
   *
   * @param data java collection
   * @param <A> value
   * @return scala collection
   */
  public static <A> List<A> asJava(final scala.collection.Seq<A> data) {
    return JavaConverters.seqAsJavaList(data);
  }
}
