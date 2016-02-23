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

package com.mongodb.spark.connection

import com.mongodb.spark.RequiresMongoDB

class MongoClientRefCounterSpec extends RequiresMongoDB {

  "MongoClientRefCounter" should "count references as expected" in {
    val counter = new MongoClientRefCounter()

    counter.canAcquire(mongoClient) should equal(false)
    counter.acquire(mongoClient)
    counter.canAcquire(mongoClient) should equal(true)

    counter.release(mongoClient) should equal(1)
    counter.release(mongoClient) should equal(0)

    counter.canAcquire(mongoClient) should equal(false)
  }

  it should "be able to acquire multiple times" in {
    val counter = new MongoClientRefCounter()
    counter.acquire(mongoClient)
    counter.acquire(mongoClient)
    counter.release(mongoClient, 2) should equal(0)
  }

  it should "throw an exception for invalid releases of a MongoClient" in {
    val counter = new MongoClientRefCounter()
    an[IllegalStateException] should be thrownBy counter.release(mongoClient)

    counter.acquire(mongoClient)
    an[IllegalStateException] should be thrownBy counter.release(mongoClient, 2)
  }
}
