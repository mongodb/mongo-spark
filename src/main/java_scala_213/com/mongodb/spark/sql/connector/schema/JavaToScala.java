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
 * limitations under the License.
 *
 */

package com.mongodb.spark.sql.connector.schema;

import java.util.List;
import java.util.Map;

import scala.jdk.javaapi.CollectionConverters$;

/** Utils object to convert Java To Scala to enable cross build */
public final class JavaToScala {
  private JavaToScala() {}

  /**
   * Wrapper to convert java map to scala map (to be able to build cross scala version)
   *
   * @param base java collection
   * @param <K> key
   * @param <V> value
   * @return scala collection
   */
  public static <K, V> scala.collection.Map<K, V> asScala(final Map<K, V> base) {
    return CollectionConverters$.MODULE$.asScala(base);
  }

  /**
   * Wrapper to convert java list to scala seq (to be able to build cross scala version)
   *
   * @param base java collection
   * @param <A> value
   * @return scala collection
   */
  public static <A> scala.collection.Seq<A> asScala(final List<A> base) {
    return CollectionConverters$.MODULE$.asScala(base).toSeq();
  }
}
