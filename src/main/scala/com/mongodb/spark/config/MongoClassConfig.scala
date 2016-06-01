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

package com.mongodb.spark.config

import java.util

/**
 * Mongo Spark Configurations
 *
 * Defines helper methods for transforming or updating configurations.
 *
 * @see [[ReadConfig]]
 * @since 1.0
 */
trait MongoClassConfig extends Serializable {

  /**
   * Defines Self as a type that can be used to return a copy of the object i.e. a different instance of the same type
   */
  type Self <: MongoClassConfig

  /**
   * Creates a new config with the options applied
   *
   * @param key the configuration key
   * @param value the configuration value
   * @return an updated config
   */
  def withOption(key: String, value: String): Self

  /**
   * Creates a new config with the options applied
   *
   * @param options a map of options to be applied to the config
   * @return an updated config
   */
  def withOptions(options: collection.Map[String, String]): Self

  /**
   * Creates a map of options representing this config.
   *
   * @return the map representing the config
   */
  def asOptions: collection.Map[String, String]

  /**
   * Creates a new config with the options applied
   *
   * @param options a map of options to be applied to the config
   * @return an updated config
   */
  def withOptions(options: util.Map[String, String]): Self

  /**
   * Creates a map of options representing the configuration
   *
   * @return the map representing the configuration values
   */
  def asJavaOptions: util.Map[String, String]

}

