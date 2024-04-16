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
package com.mongodb.spark.sql.connector;

/**
 * An adapter for SupportsNamespaces as the API for dropNamespace changed between versions
 */
interface SupportsNamespacesAdapter {

  /**
   * Drop a database.
   *
   * @param namespace (database) a multi-part namespace
   * @return true if the namespace (database) was dropped
   */
  boolean dropNamespace(String[] namespace);

  /**
   * Drop a namespace from the catalog with cascade mode
   *
   * @param namespace a multi-part namespace
   * @param cascade ignored for mongodb
   * @return true if the namespace was dropped
   */
  boolean dropNamespace(String[] namespace, boolean cascade);
}
