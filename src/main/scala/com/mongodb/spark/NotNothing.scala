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

import scala.annotation.implicitNotFound
import com.mongodb.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 *
 * `NotNothing` bottom type constraint for methods allowing a generic type that is not `Nothing`.
 */
@DeveloperApi
@implicitNotFound("You must explicitly provide a type.")
trait NotNothing[T]

/**
 * :: DeveloperApi ::
 *
 * `NotNothing` bottom type constraint for methods allowing a generic type that is not `Nothing`.
 */
@DeveloperApi
object NotNothing {
  private val evidence: NotNothing[Any] = new Object with NotNothing[Any]
  implicit def notNothingEv[T](implicit n: T =:= T): NotNothing[T] =
    evidence.asInstanceOf[NotNothing[T]]
}
