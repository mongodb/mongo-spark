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

package com.mongodb.spark

import java.io.Serializable

import com.mongodb.MongoClient

/**
 * A factory for creating MongoClients
 *
 * '''Note:''' Care should be taken to implement an `equals` method to ensure that the `MongoClientCache` can cache and reuse the resulting
 * `MongoClient`.
 *
 * @since 1.0
 */
trait MongoClientFactory extends Serializable {

  /**
   * Creates a `MongoClient`
   *
   * @return the new `MongoClient`
   */
  def create(): MongoClient

}
