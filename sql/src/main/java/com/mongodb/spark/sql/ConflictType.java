/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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
 */

package com.mongodb.spark.sql;

import org.apache.spark.sql.types.DataType;

/**
 * A tombstone type to mark conflicting DataTypes in the document structure to schema
 * translation process.
 */
public final class ConflictType extends DataType {
    /**
     * Gets the ConflictType object.
     */
    public static final ConflictType CONFLICT_TYPE = new ConflictType();

    private ConflictType() {}

    @Override
    public int defaultSize() {
        return 0;
    }

    @Override
    public DataType asNullable() {
        return this;
    }

    @Override
    public String toString() {
        return "ConflictType";
    }
}
