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

import java.util.Date;

import static com.mongodb.spark.sql.fieldTypes.api.java.Assertions.notNull;

/**
 * A Java bean representing the Bson Timestamp type
 *
 * @since 1.0
 */
public final class Timestamp {
    private int time;
    private int inc;

    /**
     * Construct a new instance
     */
    public Timestamp() {
    }

    /**
     * Construct a new instance
     *
     * @param date the date representing the seconds since epoch
     */
    public Timestamp(final Date date, final int inc) {
        this((int) notNull("date", date).getTime() / 1000, inc);
    }

    /**
     * Construct a new instance
     *
     * @param date the date representing the seconds since epoch
     */
    public Timestamp(final java.sql.Date date, final int inc) {
        this((int) notNull("date", date).getTime() / 1000, inc);
    }

    /**
     * Construct a new instance
     *
     * @param time the time in seconds since epoch
     * @param inc an incrementing ordinal for operations within a given second
     */
    public Timestamp(final int time, final int inc) {
        this.time = time;
        this.inc = inc;
    }

    public int getTime() {
        return time;
    }

    public void setTime(final int time) {
        this.time = time;
    }

    public int getInc() {
        return inc;
    }

    public void setInc(final int inc) {
        this.inc = inc;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Timestamp that = (Timestamp) o;
        if (time != that.time) {
            return false;
        }
        return inc == that.inc;
    }

    @Override
    public int hashCode() {
        int result = time;
        result = 31 * result + inc;
        return result;
    }
}
