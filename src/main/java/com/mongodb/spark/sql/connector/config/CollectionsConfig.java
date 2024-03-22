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

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.fail;
import static java.lang.String.format;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.StreamSupport.stream;

import com.mongodb.spark.sql.connector.MongoTableProvider;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.jetbrains.annotations.ApiStatus;

/**
 * A configuration of the set of collections for {@linkplain Scan scanning} from.
 *
 * @see AbstractMongoConfig#getCollectionsConfig()
 * @see MongoConfig#getCollectionName()
 * @see AbstractMongoConfig#COLLECTION_NAME_CONFIG
 */
@ApiStatus.Internal
public final class CollectionsConfig {
  private static final char ALL_PATTERN = '*';

  private final Set<String> names;

  static CollectionsConfig parse(final String raw) throws ParsingException {
    assertNotNull(raw);
    assertFalse(raw.isEmpty());
    final Set<String> names;
    if (raw.length() == 1 && raw.charAt(0) == ALL_PATTERN) {
      names = emptySet();
    } else {
      names = stream(new CollectionNameSpliterator(raw), false).collect(Collectors.toSet());
      assertFalse(names.isEmpty());
    }
    return new CollectionsConfig(names);
  }

  private CollectionsConfig(final Set<String> names) {
    this.names = unmodifiableSet(names);
  }

  /**
   * Gets the type of the configuration.
   * @return The type of the configuration.
   */
  @ApiStatus.Internal
  public Type getType() {
    if (names.isEmpty()) {
      return Type.ALL;
    } else if (names.size() == 1) {
      return Type.SINGLE;
    } else {
      return Type.MULTIPLE;
    }
  }

  String getName() {
    assertTrue(getType() == Type.SINGLE);
    return names.iterator().next();
  }

  /**
   * It is {@linkplain Collection#isEmpty() empty} iff {@link #getType()} is {@link CollectionsConfig.Type#ALL},
   * because in this case the collections are detected automatically and the set of collections may change while scanning,
   * if collections are created or dropped.
   *
   * @return An unmodifiable {@link Collection}.
   */
  @ApiStatus.Internal
  public Collection<String> getNames() {
    return names;
  }

  String getPartialNamespaceDescription() {
    switch (getType()) {
      case SINGLE:
        return getName();
      case MULTIPLE:
        return names.toString();
      case ALL:
        return String.valueOf(ALL_PATTERN);
      default:
        throw fail();
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CollectionsConfig that = (CollectionsConfig) o;
    return Objects.equals(names, that.names);
  }

  @Override
  public int hashCode() {
    return Objects.hash(names);
  }

  /**
   * The type of {@link CollectionsConfig}.
   * Some types may not be supported by some {@linkplain Scan scanning} modes.
   */
  @ApiStatus.Internal
  public enum Type {
    /**
     * Scan a {@linkplain MongoConfig#getCollectionName() single collection}.
     * This type is supported by
     * {@linkplain Scan#toBatch() batch queries},
     * {@linkplain Scan#toMicroBatchStream(String) micro-batch streams},
     * {@linkplain Scan#toContinuousStream(String) continuous streams}.
     */
    @ApiStatus.Internal
    SINGLE,
    /**
     * Scan multiple collections.
     * This type is supported by
     * {@linkplain Scan#toMicroBatchStream(String) micro-batch streams},
     * {@linkplain Scan#toContinuousStream(String) continuous streams}.
     */
    @ApiStatus.Internal
    MULTIPLE,
    /**
     * Scan all collections in the {@linkplain MongoConfig#getDatabaseName() database}.
     * This type is supported by
     * {@linkplain Scan#toMicroBatchStream(String) micro-batch streams},
     * {@linkplain Scan#toContinuousStream(String) continuous streams}.
     * <p>
     * {@linkplain MongoTableProvider#inferSchema(CaseInsensitiveStringMap) Schema inference} happens at the beginning
     * of scanning, and does not take into account collections that may be created while scanning, despite those collections being
     * scanned after being created. If you want to scan a fixed set of collections, use {@link #MULTIPLE}.</p>
     */
    @ApiStatus.Internal
    ALL
  }

  static final class ParsingException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private ParsingException(final String message) {
      super(message);
    }
  }

  private static final class CollectionNameSpliterator implements Spliterator<String> {
    private static final char SEPARATOR = ',';
    private static final char ESCAPE = '\\';
    private static final char[] ESCAPABLE = {SEPARATOR, ESCAPE, ALL_PATTERN};

    private final String source;
    private int lastParsedIdx;

    CollectionNameSpliterator(final String source) {
      assertFalse(source.isEmpty());
      this.source = source;
      lastParsedIdx = -1;
    }

    @Override
    public boolean tryAdvance(final Consumer<? super String> action) throws ParsingException {
      int lastIdx = source.length() - 1;
      if (lastParsedIdx == lastIdx) {
        return false;
      }
      StringBuilder elementBuilder = new StringBuilder();
      while (true) {
        int fistUnparsedIdx = lastParsedIdx + 1;
        int separatorIdx = source.indexOf(SEPARATOR, fistUnparsedIdx);
        int escapeIdx = source.indexOf(ESCAPE, fistUnparsedIdx);
        if (separatorIdx < 0 && escapeIdx < 0) {
          // the current element is the last one
          elementBuilder.append(source, fistUnparsedIdx, lastIdx + 1);
          lastParsedIdx = lastIdx;
          break;
        }
        assertFalse(separatorIdx == escapeIdx);
        if (escapeIdx < 0 || (separatorIdx >= 0 && separatorIdx < escapeIdx)) {
          // parse up to the `separatorIdx`, as nothing is escaped in the current element
          if ((fistUnparsedIdx == separatorIdx && elementBuilder.length() == 0)
              || separatorIdx == lastIdx) {
            throw emptyElementException(separatorIdx);
          }
          elementBuilder.append(source, fistUnparsedIdx, separatorIdx);
          lastParsedIdx = separatorIdx;
          break;
        } else {
          // unescape and continue parsing the current element
          if (escapeIdx == lastIdx) {
            throw lonelyEscapeException(escapeIdx);
          }
          int escapedIdx = escapeIdx + 1;
          char escaped = source.charAt(escapedIdx);
          if (escapable(escaped)) {
            elementBuilder.append(source, fistUnparsedIdx, escapeIdx).append(escaped);
            lastParsedIdx = escapedIdx;
          } else {
            throw unescapableException(escaped, escapedIdx);
          }
        }
      }
      assertFalse(elementBuilder.length() == 0);
      action.accept(elementBuilder.toString());
      return true;
    }

    @Override
    public Spliterator<String> trySplit() {
      return null;
    }

    @Override
    public long estimateSize() {
      return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
      return IMMUTABLE | NONNULL;
    }

    private static boolean escapable(final char c) {
      for (char escapable : ESCAPABLE) {
        if (c == escapable) {
          return true;
        }
      }
      return false;
    }

    private ParsingException emptyElementException(final int separatorIdx) {
      return new ParsingException(format(
          "Empty elements are not allowed. Something is wrong at the index %d: \"%s\"",
          separatorIdx, source));
    }

    private ParsingException lonelyEscapeException(final int escapeIdx) {
      return new ParsingException(format(
          "The '%c' character at the index %d does not escape anything: \"%s\"",
          ESCAPE, escapeIdx, source));
    }

    private ParsingException unescapableException(final char escaped, final int escapedIdx) {
      throw new ParsingException(format(
          "The '%c' character at the index %d must not be escaped: \"%s\"",
          escaped, escapedIdx, source));
    }
  }
}
