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

package com.mongodb.spark;

import org.bson.Document;

import java.util.ArrayList;

/**
 * Represents a course data model.
 */
public class Course {
    private String courseName;
    private ArrayList<String> courseTeachers;

    /**
     * Constructs a new instance.
     *
     * @param name  the name of the course
     */
    public Course(final String name) {
        courseName = name;
        courseTeachers = new ArrayList<>();
    }

    /**
     * Adds a teacher to the course.
     *
     * @param name  the name of the teacher
     */
    public void addTeacher(final String name) {
        courseTeachers.add(name);
    }

    /**
     * Generates a document for the course instance.
     *
     * @return  the document representing the course data model
     */
    public Document toDocument() {
        return new Document("course", courseName).append("teachers", courseTeachers);
    }
}
