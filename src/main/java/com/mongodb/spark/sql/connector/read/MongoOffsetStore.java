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
package com.mongodb.spark.sql.connector.read;

import static java.lang.String.format;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.spark.sql.connector.exceptions.ConfigException;

/** The Mongo offset store for streams. */
class MongoOffsetStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoOffsetStore.class);
  private final Path path;
  private final FileSystem fs;
  private MongoOffset offset;

  /**
   * Instantiates a new Mongo offset store.
   *
   * @param conf the conf
   * @param location the location
   * @param offset the offset
   */
  MongoOffsetStore(final Configuration conf, final String location, final MongoOffset offset) {
    try {
      this.fs = FileSystem.get(URI.create(location), conf);
      this.path = new Path(URI.create(location));
      this.offset = offset;
    } catch (IOException e) {
      throw new ConfigException(
          format("Unable to initialize the MongoOffsetStore: %s", location), e);
    }
  }

  /**
   * Initial offset Mongo offset.
   *
   * @return the Mongo offset
   */
  public MongoOffset initialOffset() {
    boolean exists;
    try {
      exists = fs.exists(path);
    } catch (IOException e) {
      throw new ConfigException(
          format("Unable to determine if the checkpoint location exists: %s", path), e);
    }

    if (exists) {
      try (FSDataInputStream in = fs.open(path)) {
        byte[] buf = IOUtils.toByteArray(in);
        offset = fromJson(new String(buf, StandardCharsets.UTF_8));
      } catch (IOException exception) {
        throw new ConfigException(format("Failed to parse offset from: %s", path), exception);
      }
    } else {
      try (FSDataOutputStream out = fs.create(path, true)) {
        out.write(offset.json().getBytes(StandardCharsets.UTF_8));
      } catch (IOException exception) {
        throw new ConfigException(format("Failed to create new offset to: %s", path), exception);
      }
    }
    LOGGER.info("Initial offset: {}", offset.json());
    return offset;
  }

  /**
   * Update offset.
   *
   * @param offset the offset
   */
  public void updateOffset(final MongoOffset offset) {
    try (FSDataOutputStream out = fs.create(path, true)) {
      out.write(offset.json().getBytes(StandardCharsets.UTF_8));
    } catch (IOException exception) {
      throw new ConfigException(
          format("Failed to update new offset to: %s at %s", offset.json(), path), exception);
    }
  }

  MongoOffset fromJson(final String json) {
    return MongoOffset.fromJson(json);
  }
}
