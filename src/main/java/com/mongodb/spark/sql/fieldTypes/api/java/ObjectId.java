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
 * A Java bean representing the Bson ObjectId type
 *
 * @since 1.0
 */
public final class ObjectId {
    private String oid;

    /**
     * Construct a new instance
     */
    public ObjectId() {
    }

    /**
     * Construct a new instance
     *
     * @param oid the ObjectId
     */
    public ObjectId(final org.bson.types.ObjectId oid) {
        this(Assertions.notNull("oid", oid).toHexString());
    }

    /**
     * Construct a new instance
     *
     * @param oid the ObjectId hexString
     */
    public ObjectId(final String oid) {
        this.oid = Assertions.notNull("oid", oid);
    }

    public String getOid() {
        return oid;
    }

    public void setOid(final String oid) {
        this.oid = Assertions.notNull("oid", oid);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ObjectId that = (ObjectId) o;
        return oid != null ? oid.equals(that.oid) : that.oid == null;
    }

    @Override
    public int hashCode() {
        return oid != null ? oid.hashCode() : 0;
    }
}
