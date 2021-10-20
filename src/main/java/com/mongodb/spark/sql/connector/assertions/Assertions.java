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

package com.mongodb.spark.sql.connector.assertions;

import java.util.function.Function;
import java.util.function.Supplier;

/** Assertions to validate inputs */
public final class Assertions {

  /**
   * Ensures the validity of state
   *
   * @param stateCheck the supplier of the state check
   * @param errorMessage the error message if the supplier fails
   * @throws IllegalStateException if the state check fails
   */
  public static void ensureState(final Supplier<Boolean> stateCheck, final String errorMessage) {
    if (!stateCheck.get()) {
      throw new IllegalStateException(errorMessage);
    }
  }

  /**
   * Ensures the validity of arguments
   *
   * @param argumentCheck the supplier of the argument check
   * @param errorMessage the error message if the supplier fails
   * @throws IllegalArgumentException if the argument check fails
   */
  public static void ensureArgument(
      final Supplier<Boolean> argumentCheck, final String errorMessage) {
    if (!argumentCheck.get()) {
      throw new IllegalArgumentException(errorMessage);
    }
  }

  /**
   * Checks the validity of a value
   *
   * @param value the value to check
   * @param stateCheck the supplier of the state check
   * @param errorMessage the error message if the supplier fails
   * @param <T> the type of the value being checked
   * @return the value or throw an {@code IllegalStateException} if the value is invalid
   * @throws IllegalStateException if the state check fails
   */
  public static <T> T ensureIsValid(
      final T value, final Function<T, Boolean> stateCheck, final String errorMessage) {
    if (!stateCheck.apply(value)) {
      throw new IllegalStateException(errorMessage);
    }
    return value;
  }

  private Assertions() {}
}
