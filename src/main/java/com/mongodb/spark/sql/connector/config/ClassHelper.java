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

package com.mongodb.spark.sql.connector.config;

import static java.lang.String.format;

import java.util.List;

import org.jetbrains.annotations.ApiStatus;

import com.mongodb.spark.sql.connector.assertions.Assertions;
import com.mongodb.spark.sql.connector.exceptions.ConfigException;

@ApiStatus.Internal
final class ClassHelper {

  @SuppressWarnings("unchecked")
  static <T> T createAndConfigInstance(
      final String configKey,
      final String className,
      final Class<T> clazz,
      final MongoConfig mongoConfig) {

    T instance = createInstance(configKey, className, clazz);
    Assertions.ensureArgument(
        () -> instance instanceof Configurable<?>,
        format("The class '%s' must extend Configurable.class", clazz.getSimpleName()));
    return ((Configurable<T>) instance).configure(mongoConfig);
  }

  @SuppressWarnings("unchecked")
  static <T> T createInstance(
      final String configKey, final String className, final Class<T> clazz) {
    return createInstance(
        configKey,
        className,
        clazz,
        () -> (T) Class.forName(className).getConstructor().newInstance());
  }

  @SuppressWarnings("unchecked")
  static <T> T createInstance(
      final String configKey,
      final String className,
      final Class<T> clazz,
      final List<Class<?>> constructorArgs,
      final List<Object> initArgs) {
    return createInstance(
        configKey,
        className,
        clazz,
        () ->
            (T)
                Class.forName(className)
                    .getConstructor(constructorArgs.toArray(new Class<?>[0]))
                    .newInstance(initArgs.toArray(new Object[0])));
  }

  private static <T> T createInstance(
      final String configKey,
      final String className,
      final Class<T> clazz,
      final ClassCreator<T> cc) {
    try {
      return cc.init();
    } catch (ClassCastException e) {
      throw new ConfigException(
          configKey,
          className,
          format("Contract violation class doesn't implement: '%s'", clazz.getSimpleName()));
    } catch (ClassNotFoundException e) {
      throw new ConfigException(
          configKey, className, format("Class not found: %s", e.getMessage()));
    } catch (NoSuchMethodException e) {
      throw new ConfigException(
          configKey,
          className,
          format(
              "Class could not be initialized, no public default constructor: %s", e.getMessage()));
    } catch (Exception e) {
      if (e.getCause() instanceof ConfigException) {
        throw (ConfigException) e.getCause();
      }
      throw new ConfigException(configKey, className, e.getMessage());
    }
  }

  @FunctionalInterface
  interface ClassCreator<T> {
    T init() throws Exception;
  }

  private ClassHelper() {}
}
