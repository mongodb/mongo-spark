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

package com.mongodb.spark.sql.fieldTypes.api.java;

/**
 * A Java bean representing the Bson MaxKey type
 *
 * @since 1.0
 */
public final class MaxKey {
    private int maxKey;

    /**
     * Construct a new instance
     */
    public MaxKey() {
        this(1);
    }

    /**
     * Construct a new instance
     *
     * @param maxKey data representing the maxKey
     */
    public MaxKey(final int maxKey) {
        this.maxKey = maxKey;
    }

    public int getMaxKey() {
        return maxKey;
    }

    public void setMaxKey(final int maxKey) {
        this.maxKey = maxKey;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MaxKey that = (MaxKey) o;
        return maxKey == that.maxKey;
    }

    @Override
    public int hashCode() {
        return maxKey;
    }
}
