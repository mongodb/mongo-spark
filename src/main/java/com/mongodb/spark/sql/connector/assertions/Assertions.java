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

import com.mongodb.spark.sql.connector.exceptions.ConfigException;
import java.util.function.Predicate;
import java.util.function.Supplier;

/** Assertions to validate inputs */
public final class Assertions {

  /**
   * Ensures the validity of state
   *
   * @param stateCheck the supplier of the state check
   * @param errorMessageSupplier the supplier of the error message if the predicate
   * @throws IllegalStateException if the state check fails
   */
  public static void ensureState(
      final Supplier<Boolean> stateCheck, final Supplier<String> errorMessageSupplier) {
    if (!stateCheck.get()) {
      throw new IllegalStateException(errorMessageSupplier.get());
    }
  }

  /**
   * Ensures the validity of arguments
   *
   * @param argumentCheck the supplier of the argument check
   * @param errorMessageSupplier the supplier of the error message if the predicate fails
   * @throws IllegalArgumentException if the argument check fails
   */
  public static void ensureArgument(
      final Supplier<Boolean> argumentCheck, final Supplier<String> errorMessageSupplier) {
    if (!argumentCheck.get()) {
      throw new IllegalArgumentException(errorMessageSupplier.get());
    }
  }

  /**
   * Checks the validity of a value
   *
   * @param value the value to check
   * @param predicate the predicate
   * @param errorMessageSupplier the supplier of the error message if the predicate fails
   * @param <T> the type of the value being checked
   * @return the value or throw an {@code ConfigException} if the value is invalid
   * @throws ConfigException if the predicate returns false
   */
  public static <T> T validateConfig(
      final T value, final Predicate<T> predicate, final Supplier<String> errorMessageSupplier) {
    if (!predicate.test(value)) {
      throw new ConfigException(errorMessageSupplier.get());
    }
    return value;
  }

  /**
   * Checks the validity of a value
   *
   * @param valueSupplier the supplier of the value
   * @param errorMessageSupplier the supplier of the error message if the predicate fails
   * @param <T> the type of the value being checked
   * @return the value or throw an {@code ConfigException} if the supplier throws an exception
   * @throws ConfigException if the supplier throws an exception
   */
  public static <T> T validateConfig(
      final Supplier<T> valueSupplier, final Supplier<String> errorMessageSupplier) {
    try {
      return valueSupplier.get();
    } catch (RuntimeException ex) {
      throw new ConfigException(errorMessageSupplier.get(), ex);
    }
  }

  private Assertions() {}
}
