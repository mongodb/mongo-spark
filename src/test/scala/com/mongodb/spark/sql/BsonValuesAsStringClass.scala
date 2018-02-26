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

package com.mongodb.spark.sql

case class BsonValuesAsStringClass(nullValue: String, int32: String, int64: String, bool: String, date: String, dbl: String,
                                   decimal: String, string: String, minKey: String, maxKey: String, objectId: String, code: String,
                                   codeWithScope: String, regex: String, symbol: String, timestamp: String, undefined: String,
                                   binary: String, oldBinary: String, arrayInt: String, document: String, dbPointer: String)
