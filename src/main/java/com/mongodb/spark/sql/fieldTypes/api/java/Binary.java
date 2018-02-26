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

import org.bson.BsonBinarySubType;

import java.util.Arrays;

/**
 * A Java bean representing the Bson Binary type
 *
 * @since 1.0
 */
public final class Binary {
    private byte subType;
    private byte[] data;

    /**
     * Construct a new instance
     */
    public Binary() {
    }

    /**
     * Construct a new instance
     *
     * @param data the binary data
     */
    public Binary(final byte[] data) {
        this(BsonBinarySubType.BINARY.getValue(), data);
    }

    /**
     * Construct a new instance
     *
     * @param subType the binary subtype
     * @param data the binary data
     */
    public Binary(final byte subType, final byte[] data) {
        this.subType = subType;
        this.data = data;
    }

    public byte getSubType() {
        return subType;
    }

    public void setSubType(final byte subType) {
        this.subType = subType;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(final byte[] data) {
        this.data = data;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Binary that = (Binary) o;

        if (subType != that.subType) {
            return false;
        }
        return Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        int result = (int) subType;
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }
}
