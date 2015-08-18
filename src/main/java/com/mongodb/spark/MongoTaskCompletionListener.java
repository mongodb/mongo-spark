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
 *
 */

package com.mongodb.spark;

import com.mongodb.client.MongoCursor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.TaskContext;
import org.apache.spark.util.TaskCompletionListener;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An implementation of a TaskCompletionListener that closes a connection
 * once the task has been completed.
 */
public class MongoTaskCompletionListener implements TaskCompletionListener {
    private static final Log LOG = LogFactory.getLog(MongoTaskCompletionListener.class);

    private MongoCursor cursor;

    /**
     * Constructs a new instance.
     *
     * @param cursor the cursor
     */
    public MongoTaskCompletionListener(final MongoCursor cursor) {
        this.cursor = notNull("cursor", cursor);
    }

    @Override
    public void onTaskCompletion(final TaskContext context) {
        LOG.debug("Closing cursor");

        this.cursor.close();
    }
}
