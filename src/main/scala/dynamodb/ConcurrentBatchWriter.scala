/*
 * Copyright 2012-2015 Pellucid Analytics
 * Copyright 2015 Daniel W. H. James
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

package com.github.dwhjames.awswrap.dynamodb

import java.{util => ju}
import java.util.{concurrent => juc}
import java.util.concurrent.{atomic => juca}
import java.util.concurrent.{locks => jucl}

import com.amazonaws.{AmazonClientException, AmazonServiceException, ClientConfiguration}
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider}
import com.amazonaws.internal.StaticCredentialsProvider
import com.amazonaws.retry.RetryUtils
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model.{BatchWriteItemRequest, ProvisionedThroughputExceededException, ReturnConsumedCapacity, WriteRequest}

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
  * A record to communicate errors during batch write attempts.
  *
  * @tparam Metadata
  *     the type of the metadata tag
  * @param tableName
  *     the name of the table that was being written to
  * @param batch
  *     the list of write request that were in the failed batch
  * @param cause
  *     the exception that occurred while writing the batch
  * @param metadata
  *     the metadata that was supplied for the write group
  */
case class FailedBatch[Metadata](
    tableName: String,
    batch:     ju.List[WriteRequest],
    cause:     Throwable,
    metadata:  Metadata
)

/**
  * A multi-threaded, self-throttling, batch writer to a given DynamoDB table
  *
  * This class provides a blocking interface to write items in batch to one
  * DynamoDB table. The credentials that are provided must permit the
  * "BatchWriteItem" operation to the named table.
  *
  * @constructor Create a new concurrent batch writer
  * @param tableName
  *     The name of the DynamoDB table to write to
  * @param credentialsProvider
  *     A provider for credentials to the DynamoDB table
  * @param writeConcurrency
  *    The number of concurrent writers
  */
class ConcurrentBatchWriter(
    val tableName: String,
    val credentialsProvider: AWSCredentialsProvider,
    val writeConcurrency: Int
) {

  /**
    * Create a new concurrent batch writer
    *
    * @param tableName
    *     The name of the DynamoDB table to write to
    * @param credentials
    *     Static credentials to the DynamoDB table
    * @param writeConcurrency
    *     The number of concurrent writers
    */
  def this(tableName: String, credentials: AWSCredentials, writeConcurrency: Int) {
    this(tableName, new StaticCredentialsProvider(credentials), writeConcurrency)
  }

  // convenience type synonym
  private type Batch = ju.Map[String, ju.List[WriteRequest]]

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  // the number of milliseconds that writers will pause for
  // shared among all writers
  private val pauseDuration = new juca.AtomicLong(0L)

  // a synchronous DynamoDB client that performs no internal retry-logic
  // and has a pool of connections equal to the number of concurrent writers
  private val dynamoDBClient = new AmazonDynamoDBClient(
                                  credentialsProvider,
                                  new ClientConfiguration()
                                    .withMaxErrorRetry(0)
                                    .withMaxConnections(writeConcurrency))


  /*
   * turn an fixed capacity, array blocking queue into a buffer
   * a thread pool executor uses the non-blocking method offer,
   * so we have to subvert it to block
   */
  private class BlockingBuffer[E](capacity: Int) extends juc.ArrayBlockingQueue[E](capacity) {
    override def offer(e: E): Boolean =
      try {
        this.put(e)
        true
      } catch {
        case e: InterruptedException =>
          Thread.currentThread.interrupt()
          false
      }
    override def offer(e: E, timeout: Long, unit: juc.TimeUnit): Boolean = offer(e)
  }


  // a thread factory with custom named threads
  private class CustomThreadFactory extends juc.ThreadFactory {
    private val count = new juca.AtomicLong(0L)
    private val backingThreadFactory: juc.ThreadFactory = juc.Executors.defaultThreadFactory()
    override def newThread(r: Runnable): Thread = {
      val thread = backingThreadFactory.newThread(r)
      thread.setName(s"DynamoDB-$tableName-${count.getAndIncrement()}")
      thread
    }
  }


  // a customized thread pool executor, with a fixed number of threads
  // using the blocking buffer as the input queue and the custom thread factory
  private val writerThreadPool = new juc.ThreadPoolExecutor(
                                    0, writeConcurrency,
                                    60L, juc.TimeUnit.SECONDS,
                                    new BlockingBuffer[Runnable](2*writeConcurrency),
                                    new CustomThreadFactory())


  // implement exponential backoff using Thread.sleep
  @throws(classOf[InterruptedException])
  private def pause(attempts: Int): Unit =
    if (attempts > 0) {
      // 50, 100, 200, 400, 800
      val delay = (math.pow(2.0, attempts - 1) * 50.0).toLong
      if (logger.isTraceEnabled) logger.trace(s"backing off for $delay msecs.")
      Thread.sleep(delay)
    }


  // write a batch, with infinite retry in the event of throttling
  @throws(classOf[InterruptedException])
  private def writeWithBackoffRetry(batch: Batch): Int = {
    var attempts = 0
    var consumedCapacity = 0.0
    var batchRemaining = batch

    while (batchRemaining.size > 0) {
      // if attempts > 0, pause
      pause(attempts)
      attempts += 1

      try {
        // build batch request
        val request =
          if (logger.isDebugEnabled)
            new BatchWriteItemRequest()
              .withRequestItems(batchRemaining)
              .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
          else
            new BatchWriteItemRequest()
              .withRequestItems(batchRemaining)

        if (logger.isTraceEnabled)
          logger.trace(s"attempt $attempts: writing ${batchRemaining.get(tableName).size} items to table $tableName in batch ${batchRemaining.hashCode}")

        // execute request
        val result = dynamoDBClient.batchWriteItem(request)

        if (logger.isDebugEnabled) {
          val capacityConsumedByRequest = result.getConsumedCapacity().get(0).getCapacityUnits()
          logger.debug(s"Consumed $capacityConsumedByRequest units of capacity for table $tableName.")
        }

        // retrieve any unprocessed items and use as the batch to continue with
        batchRemaining = result.getUnprocessedItems()

        if (logger.isTraceEnabled && batchRemaining.size > 0) {
          logger.trace(s"${batchRemaining.get(tableName).size} items were unprocessed in attempt $attempts.")
        }
      } catch {
        case e: ProvisionedThroughputExceededException =>
          if (logger.isTraceEnabled)
            logger.trace(s"Caught ProvisionedThroughputExceededException for batchWriteItem with AWS Request ID: ${e.getRequestId()}")
          // continue in while loop, so current batchRemaining will be retried.
      }
    }

    // return the number of attempts required
    attempts
  }


   // helper to make a wrap a list of write requests up into a batch
  private def mkBatch(requests: ju.List[WriteRequest]): Batch = {
    val m = new ju.HashMap[String, ju.List[WriteRequest]](1)
    m.put(tableName, requests)
    m
  }


  /**
    * A class for managing the synchronization of a group of writes.
    *
    * This class facilitates the grouping of write requests.
    *
    * @tparam Metadata
    *     the type of the metadata tag
    * @param metadata
    *     the metadata that tags this group of writes
    * @param errorQueue
    *     a queue on which to return errors from workers in the thread pool (must be thread-safe)
    */
  class WriteGroup[Metadata](
      metadata:   Metadata,
      errorQueue: ju.Queue[FailedBatch[Metadata]]
  ) {

    private var uncompletedWrites = 0
    private val uncompletedWritesLock: jucl.Lock = new jucl.ReentrantLock()
    private val zeroWritesOutstanding: jucl.Condition = uncompletedWritesLock.newCondition()

    @volatile
    private var errorFree = true


    // uninterruptibly increment the shared counter
    private def recordQueuedBatchWriteRunnable(): Unit = {
      uncompletedWritesLock.lock()
      try {
        uncompletedWrites += 1
      } finally {
        uncompletedWritesLock.unlock()
      }
    }


    /* uninterruptibly decrement the shared counter
     * and signal those waiting when counter is zero
     * note, we signalAll as there could be multiple
     * threads waiting on this condition.
     */
    private def signalBatchWriteRunnableCompletion(): Unit = {
      uncompletedWritesLock.lock()
      try {
        uncompletedWrites -= 1
        if (uncompletedWrites == 0)
          zeroWritesOutstanding.signalAll()
      } finally {
        uncompletedWritesLock.unlock()
      }
    }

    /**
      * Wait until all writes that have been queued have been written.
      *
      * Block the calling thread until all writes in the queue have been
      * run, either with success or failure. If all the queued writes
      * complete successfully, then this method returns true. If false,
      * then failed batches will have been recorded in the error queue.
      *
      * Note that this is an blocking call
      *
      * @return true if the group of writes completed without errors
      * @throws InterruptedException if interrupted while waiting
      */
    @throws(classOf[InterruptedException])
    def awaitCompletionOfAllWrites(): Boolean = {
      uncompletedWritesLock.lockInterruptibly()
      try {
        try {
          while (uncompletedWrites != 0)
            zeroWritesOutstanding.await()
        } catch {
          case e: InterruptedException =>
            zeroWritesOutstanding.signal() // propagate to non-interrupted thread
            throw e
        }
        errorFree
      } finally {
        uncompletedWritesLock.unlock()
      }
    }

    /*
     * Construct a runnable for the batch writer thread pool.
     *
     * This runnable will infinitely retry the encapsulated batch
     * on throttling exceptions, using exponential backoff.
     * This runnable will begin by sleeping for the duration that
     * is stored in the shared variable for the thread pool.
     * On completion, success or failure, the
     */
    private class BatchWriteRunnable(batch: Batch) extends Runnable {

      override def run(): Unit = {
        try {
          val dur = pauseDuration.get()
          if (logger.isTraceEnabled)
            logger.trace(s"pausing for $dur msecs.")
          Thread.sleep(dur)

          val attempts =
            try {
              writeWithBackoffRetry(batch)
            } catch {
              case e: AmazonServiceException if RetryUtils.isRequestEntityTooLargeException(e) =>
                // if request exceeded the 1Mb request limit
                val requests = batch.get(tableName)
                val size = requests.size
                val mid  = size / 2

                logger.warn(s"batch for table $tableName exceeded the request size limit! ${batch.hashCode}")

                // split it into two and repeat seperately
                val f_attempts = writeWithBackoffRetry(mkBatch(requests.subList(0, mid)))
                val s_attempts = writeWithBackoffRetry(mkBatch(requests.subList(mid, size)))

                // return the max of the attempts, and sum of consumed capacity
                math.max(f_attempts, s_attempts)
            }

          if (logger.isTraceEnabled)
            logger.trace(s"$attempts attempts required to write batch ${batch.hashCode}")

          if (attempts > 1) {
            val dur = 5L * writeConcurrency
            pauseDuration.getAndAdd(dur)
            if (logger.isTraceEnabled)
              logger.trace(s"increasing pause by $dur msecs")
          } else {
            var dur = pauseDuration.get()
            while (!pauseDuration.compareAndSet(dur, math.max(0, dur - 1))) { dur = pauseDuration.get() }
            if (logger.isTraceEnabled)
              logger.trace(s"decrementing pause")
          }

        } catch {
          case e: InterruptedException =>
            Thread.currentThread.interrupt()
          case e: AmazonClientException =>
            errorFree = false
            logger.error(s"AWS error occured when attempting to write to table $tableName", e)
            errorQueue.offer(
              FailedBatch[Metadata](tableName, batch.get(tableName), e, metadata))
          case e: Throwable =>
            // log and rethrow any other exceptions
            // AmazonClientException should cover all error cases
            // with regards to the batch write call
            logger.error(s"Unexpected error when attempting to write to table $tableName", e)
            throw e
        } finally {
          signalBatchWriteRunnableCompletion()
        }

      }
    }


    /**
      * Add an Iterable collection of WriteRequests to the writer queue.
      *
      * Blockingly add write requests to the queue of writes. If the
      * collection is larger than 25, then the writes are queued in
      * groups of 25.
      *
      * @param requests
      *     an iterable collection of WriteRequests
      * @throws InterruptedException if interrupted while waiting
      */
    @throws(classOf[InterruptedException])
    def queueWriteRequests(requests: Iterable[WriteRequest]): Unit = {
      val iter = requests.iterator

      while (iter.hasNext) {

        val writeRequests = new ju.ArrayList[WriteRequest](25)
        var i = 0
        while (iter.hasNext && i < 25) {
          writeRequests.add(iter.next())
          i += 1
        }

        recordQueuedBatchWriteRunnable()
        writerThreadPool.execute(new BatchWriteRunnable(mkBatch(writeRequests)))

        if (logger.isTraceEnabled) {
          logger.trace(s"queued ${writeRequests.size} write requests to table $tableName")
          logger.trace(s"write buffer for table $tableName has size ${writerThreadPool.getQueue.size}")
        }
      }
    }

  }


  /**
    * Create a WriteGroup object to be used for a group of write requests that
    * have a metadata tag in common.
    *
    * This creates a view on to the concurrent batch writer, so that a collection
    * of writes can be grouped together. The writes that are queued are tagged
    * with the given metadata, so that errors can be reported informatively,
    * and when used in tandem with [WriteGroup.awaitCompletionOfAllWrites],
    * one can wait until all writes in a group have completed.
    *
    * @tparam Metadata
    *     the type of the metadata tag
    * @param metadata
    *     the metadata to tag this group with
    * @param errorQueue
    *     the error queue to report errors on
    * @return a new write group
    */
  def createWriteGroup[Metadata](
      metadata:   Metadata,
      errorQueue: ju.Queue[FailedBatch[Metadata]]): WriteGroup[Metadata] =
    new WriteGroup[Metadata](metadata, errorQueue)


  /**
    * Perform an orderly shut down of the underlying thread pool and connection pool.
    *
    * This initiates an orderly shut down on thread and connection resources.
    * The write queue is closed, and write batches that were previously submitted
    * are given 60 seconds to be run and complete. After this time, all running
    * writes are cancelled, and are given 60 seconds to terminate.
    * The underlying DynamoDB client and its connection pool is also shutdown.
    */
  def shutdownAndAwaitTermination(): Unit = {
    writerThreadPool.shutdown() // Disable new tasks from being submitted
    logger.info(s"batch writer thread pool for table $tableName is shutting down.")
    try {
      // Wait a while for existing tasks to terminate
      if (!writerThreadPool.awaitTermination(60, juc.TimeUnit.SECONDS)) {
        writerThreadPool.shutdownNow() // Cancel currently executing tasks
        logger.warn(s"batch writer thread pool for table $tableName being forced to shutdown.")
        // Wait a while for tasks to respond to being cancelled
        if (!writerThreadPool.awaitTermination(60, juc.TimeUnit.SECONDS))
            logger.error("batch writer thread pool for table $tableName did not shutdown!")
      }
      dynamoDBClient.shutdown()
      logger.info(s"batch writer DynamoDB client for table $tableName is shutting down.")
    } catch {
      case e: InterruptedException =>
        // (Re-)Cancel if current thread also interrupted
        writerThreadPool.shutdownNow()
        dynamoDBClient.shutdown()
        // Preserve interrupt status
        Thread.currentThread.interrupt()
    }
  }

}
