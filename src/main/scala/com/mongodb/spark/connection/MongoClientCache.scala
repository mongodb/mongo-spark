//scalastyle:off
/*
 * Copyright 2016 MongoDB, Inc.
 * Copyright 2014-2015, DataStax, Inc.
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
//scalastyle:on
package com.mongodb.spark.connection

import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

import com.mongodb.MongoClient
import com.mongodb.spark.{Logging, MongoClientFactory}

/**
 * A lockless cache for MongoClients.
 *
 * Allows multiple users access to MongoClients. Closes a `MongoClient` when they're are no longer used.
 *
 * @param keepAlive the duration to keep alive any given MongoClient so that it can be reused by another consumer
 */
private[spark] final class MongoClientCache(keepAlive: Duration) extends Logging {

  private val refCounter = new MongoClientRefCounter
  private val cache = new TrieMap[MongoClientFactory, MongoClient]
  private val clientToKey = new TrieMap[MongoClient, MongoClientFactory]
  private val deferredReleases = new TrieMap[MongoClient, ReleaseTask]

  @tailrec
  def acquire(mongoClientFactory: MongoClientFactory): MongoClient = {
    cache.get(mongoClientFactory) match {
      case Some(mongoClient) =>
        refCounter.canAcquire(mongoClient) match {
          case true  => mongoClient
          case false => acquire(mongoClientFactory)
        }
      case None =>
        val createdMongoClient = mongoClientFactory.create()
        logClient(createdMongoClient)
        refCounter.acquire(createdMongoClient)
        cache.putIfAbsent(mongoClientFactory, createdMongoClient) match {
          case None =>
            clientToKey.put(createdMongoClient, mongoClientFactory)
            createdMongoClient
          case Some(existingMongoClient) =>
            logClient(createdMongoClient, closing = true)
            createdMongoClient.close()
            refCounter.release(createdMongoClient)
            refCounter.canAcquire(existingMongoClient) match {
              case true  => existingMongoClient
              case false => acquire(mongoClientFactory)
            }
        }
    }
  }

  /**
   * Releases previously acquired mongoClient. Once the mongoClient is released by all threads and
   * the `releaseDelayMillis` timeout passes, the mongoClient is destroyed by calling `destroy` function and
   * removed from the cache.
   */
  def release(mongoClient: MongoClient, releaseDelay: Duration = keepAlive) {
    if (releaseDelay.toMillis == 0 || scheduledExecutorService.isShutdown) {
      releaseImmediately(mongoClient)
    } else {
      releaseDeferred(mongoClient, releaseDelay, 1)
    }
  }

  /**
   * Shuts down the background deferred `release` scheduler and forces all pending release tasks to be executed
   */
  def shutdown() {
    scheduledExecutorService.shutdown()
    while (deferredReleases.nonEmpty) {
      for ((mongoClient, task) <- deferredReleases.snapshot()) {
        if (deferredReleases.remove(mongoClient, task)) task.run()
      }
    }
  }

  private def releaseImmediately(mongoClient: MongoClient, count: Int = 1): Unit = {
    Try(refCounter.release(mongoClient, count)) match {
      case Success(0) =>
        cache.remove(clientToKey(mongoClient))
        clientToKey.remove(mongoClient)
        logClient(mongoClient, closing = true)
        mongoClient.close()
      case Failure(e) => logWarning(e.getMessage)
      case _          =>
    }
  }

  @tailrec
  private def releaseDeferred(mongoClient: MongoClient, releaseDelay: Duration, count: Int): Unit = {
    val newTime = System.nanoTime() + releaseDelay.toNanos
    val newTask = deferredReleases.remove(mongoClient) match {
      case Some(oldTask) => ReleaseTask(mongoClient, oldTask.count + count, math.max(oldTask.scheduledTime, newTime))
      case None          => ReleaseTask(mongoClient, count, newTime)
    }
    deferredReleases.putIfAbsent(mongoClient, newTask) match {
      case Some(oldTask) => releaseDeferred(mongoClient, releaseDelay, newTask.count)
      case None          =>
    }
  }

  /**
   * Called periodically by `scheduledExecutorService`
   */
  private def processPendingReleases() {
    val now = System.nanoTime()
    for ((mongoClient, task) <- deferredReleases)
      if (task.scheduledTime <= now)
        if (deferredReleases.remove(mongoClient, task)) task.run()
  }

  private val processPendingReleasesTask = new Runnable() {
    override def run() {
      processPendingReleases()
    }
  }

  private val scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
    override def newThread(r: Runnable) = {
      val thread = Executors.defaultThreadFactory().newThread(r)
      thread.setDaemon(true)
      thread
    }
  })

  // This must be high enough so it doesn't cause too much CPU usage,
  // but also low enough to allow for acceptable releaseDelayMillis resolution.
  private val period = 100
  scheduledExecutorService.scheduleAtFixedRate(processPendingReleasesTask, period, period, TimeUnit.MILLISECONDS)

  private case class ReleaseTask(mongoClient: MongoClient, count: Int, scheduledTime: Long) extends Runnable {
    override def run() {
      releaseImmediately(mongoClient, count)
    }
  }

  private def logClient(mongoClient: MongoClient, closing: Boolean = false): Unit = {
    val status = if (closing) "Closing" else "Creating"
    logInfo(s"""$status MongoClient: ${mongoClient.getServerAddressList.asScala.map(_.toString).mkString("[", ",", "]")}""")
  }

}
