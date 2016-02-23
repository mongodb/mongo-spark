//scalastyle:off
/*
 * Copyright 2016 MongoDB, Inc.
 * Copyright 2014-2015, DataStax, Inc.
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
//scalastyle:on
package com.mongodb.spark.connection

import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap

import com.mongodb.MongoClient

/**
 * Atomically counts references to MongoClients
 */
private class MongoClientRefCounter {

  private val mongoClientCounts = new TrieMap[MongoClient, Int]

  /**
   * Indicates if the `MongoClient` can be acquired.
   *
   * Atomically increases reference count only if the reference counter is already greater than 0.
   *
   * @param key the key to acquire
   * @return true if the `MongoClient` can be acquired
   */
  @tailrec
  final def canAcquire(key: MongoClient): Boolean = {
    mongoClientCounts.get(key) match {
      case Some(count) if count > 0 =>
        if (mongoClientCounts.replace(key, count, count + 1)) {
          true
        } else {
          canAcquire(key)
        }
      case _ => false
    }
  }

  /**
   * Acquires the `MongoClient`
   *
   * Atomically increases reference count by one.
   *
   * @param key the key to acquire
   */
  @tailrec
  final def acquire(key: MongoClient): Unit = {
    mongoClientCounts.get(key) match {
      case Some(count) => if (!mongoClientCounts.replace(key, count, count + 1)) acquire(key)
      case None        => if (mongoClientCounts.putIfAbsent(key, 1).isDefined) acquire(key)
    }
  }

  /**
   * Release the `MongoClient`
   *
   * Atomically decreases reference count by `n`.
   *
   * @throws IllegalStateException if the reference count before decrease is less than `n`
   * @return the MongoClient reference count
   */
  @tailrec
  final def release(key: MongoClient, n: Int = 1): Int = {
    mongoClientCounts.get(key) match {
      case Some(count) if count > n =>
        if (mongoClientCounts.replace(key, count, count - n)) {
          count - n
        } else {
          release(key, n)
        }
      case Some(count) if count == n =>
        if (mongoClientCounts.remove(key, n)) {
          0
        } else {
          release(key, n)
        }
      case _ => throw new IllegalStateException("Release without acquire for key: " + key)
    }
  }

}
