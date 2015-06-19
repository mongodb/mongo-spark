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

/**
 * Represents a grade data model.
 */
public class Grade {
    private String course;
    private String grade;
    private String student;
    private String teacher;

    /**
     * Constructs a new instance.
     *
     * @param courseName    the name of the course
     * @param gradeScore    the grade received in the course
     * @param studentName   the name of the student that took the course
     * @param teacherName   the name of the teacher that taught the course
     */
    public Grade(final String courseName, final String gradeScore, final String studentName, final String teacherName) {
        course = courseName;
        grade = gradeScore;
        student = studentName;
        teacher = teacherName;
    }

    /**
     * Generates a document for the course instance.
     *
     * @return  the document representing the grade data model
     */
    public Document toDocument() {
        return new Document("course", course)
                    .append("grade", grade)
                    .append("student", student)
                    .append("teacher", teacher);
    }
}
