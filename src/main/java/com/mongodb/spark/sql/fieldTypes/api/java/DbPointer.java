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

import org.bson.types.ObjectId;

/**
 * A Java bean representing the Bson DbPointer type
 *
 * @since 1.0
 */
public final class DbPointer {
    private String ref;
    private String oid;

    /**
     * Construct a new instance
     */
    public DbPointer() {
    }

    /**
     * Construct a new instance
     *
     * @param ref the namespace string
     * @param oid the ObjectId
     */
    public DbPointer(final String ref, final ObjectId oid) {
        this(ref, Assertions.notNull("oid", oid).toHexString());
    }

    /**
     * Construct a new instance
     *
     * @param ref the namespace string
     * @param oid the ObjectId hexString
     */
    public DbPointer(final String ref, final String oid) {
        this.ref = Assertions.notNull("ref", ref);
        this.oid = Assertions.notNull("oid", oid);
    }

    public String getRef() {
        return ref;
    }

    public void setRef(final String ref) {
        this.ref = Assertions.notNull("ref", ref);
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

        DbPointer that = (DbPointer) o;

        if (ref != null ? !ref.equals(that.ref) : that.ref != null) {
            return false;
        }
        return oid != null ? oid.equals(that.oid) : that.oid == null;
    }

    @Override
    public int hashCode() {
        int result = ref != null ? ref.hashCode() : 0;
        result = 31 * result + (oid != null ? oid.hashCode() : 0);
        return result;
    }
}
