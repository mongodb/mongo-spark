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

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GradebookTest {
    private MongoClient mongoClient = new MongoClient();
    private MongoDatabase database = mongoClient.getDatabase("gradebook");
    private MongoCollection<Document> coursesCollection = database.getCollection("courses");
    private MongoCollection<Document> gradesCollection = database.getCollection("grades");
    private MongoCollection<Document> studentsCollection = database.getCollection("students");
    private MongoCollection<Document> teachersCollection = database.getCollection("teachers");
    private Document doc = new Document("_id", 1)
                                .append("course", "cs31")
                                .append("teacher", "Fred")
                                .append("student", "Alice")
                                .append("grade", "A");

    @Before
    public void setup() {
        gradesCollection.drop();

        Gradebook.insert(gradesCollection, doc);
    }

    @Test
    public void shouldHaveInsertedInSetup() {
        ArrayList<Document> results = Gradebook.find(gradesCollection);

        Assert.assertEquals(results.size(), 1);
    }

    @Test
    public void shouldFindOne() {
        ArrayList<Document> results = Gradebook.find(gradesCollection);

        Assert.assertEquals(results.size(), 1);
    }

    @Test
    public void shouldNotFindOneWithFilter() {
        Document filter = new Document("teacher", "Bob");

        ArrayList<Document> results = Gradebook.find(gradesCollection, filter);

        Assert.assertEquals(results.size(), 0);
    }

    @Test
    public void shouldFindOneWithFilter() {
        Document filter = new Document("teacher", "Fred");

        ArrayList<Document> results = Gradebook.find(gradesCollection, filter);

        Assert.assertEquals(results.size(), 1);
    }

    @Test
    public void testUpdate() {
        Document filter = new Document("teacher", "Fred");
        Document replacement = new Document("$set", new Document("teacher", "Charlie"));

        Gradebook.updateOne(gradesCollection, filter, replacement);

        ArrayList<Document> results = Gradebook.find(gradesCollection);

        Assert.assertEquals(results.get(0), new Document("_id", 1)
                                                 .append("course", "cs31")
                                                 .append("teacher", "Charlie")
                                                 .append("student", "Alice")
                                                 .append("grade", "A"));
    }

    @Test
    public void shouldInsertOneTeacher() {
        Teacher teacher = new Teacher("David");
        teacher.addCourse("cs31");
        teacher.addCourse("cs32");

        Gradebook.insert(gradesCollection, teacher.toDocument());

        ArrayList<Document> results = Gradebook.find(gradesCollection);

        Assert.assertEquals(results.size(), 2);
    }

    @Test
    public void shouldInsertOneCourseWithOneTeacher() {
        Course course = new Course("cs31");
        course.addTeacher("Alice");

        Gradebook.insert(gradesCollection, course.toDocument());

        ArrayList<Document> results = Gradebook.find(gradesCollection);

        Assert.assertEquals(results.size(), 2);
    }

    @Test
    public void shouldInsertOneStudentWithTwoCourses() {
        Student student = new Student("Joe");
        student.addCourse("cs31", "Smallberg", "A");
        student.addCourse("cs32", "Smallberg", "F");

        Gradebook.insert(gradesCollection, student.toDocument());

        ArrayList<Document> results = Gradebook.find(gradesCollection);

        Assert.assertEquals(results.size(), 2);
    }

    @Test
    public void shouldInsertToMultipleCollections() {
        coursesCollection.drop();
        gradesCollection.drop();
        studentsCollection.drop();
        teachersCollection.drop();

        // insert to course collection
        Course course = new Course("cs31");
        course.addTeacher("Alice");
        Gradebook.insert(coursesCollection, course.toDocument());

        // insert to grade collection
        Grade grade = new Grade("cs31", "A", "Alice", "Smallberg");
        Gradebook.insert(gradesCollection, grade.toDocument());

        // insert to student collection
        Student student = new Student("Alice");
        student.addCourse("cs31", "Smallberg", "A");
        Gradebook.insert(studentsCollection, student.toDocument());

        // insert to teacher collection
        Teacher teacher = new Teacher("Smallberg");
        teacher.addCourse("cs31");
        Gradebook.insert(teachersCollection, teacher.toDocument());

        // assertions
        Assert.assertEquals(Gradebook.find(coursesCollection).size(), 1);
        Assert.assertEquals(Gradebook.find(gradesCollection).size(), 1);
        Assert.assertEquals(Gradebook.find(studentsCollection).size(), 1);
        Assert.assertEquals(Gradebook.find(teachersCollection).size(), 1);
    }
}
