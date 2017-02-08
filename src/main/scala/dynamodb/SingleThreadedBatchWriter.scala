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
import java.util.Random

import com.amazonaws.{AmazonServiceException, ClientConfiguration}
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider}
import com.amazonaws.internal.StaticCredentialsProvider
import com.amazonaws.retry.RetryUtils
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model.{BatchWriteItemRequest, ProvisionedThroughputExceededException, ReturnConsumedCapacity, WriteRequest}

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class SingleThreadedBatchWriter(
    val tableName: String,
    val credentialsProvider: AWSCredentialsProvider
) {

  private type Batch = ju.Map[String, ju.List[WriteRequest]]

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
    * a synchronous DynamoDB client, configured to perform no retry logic internally
    *
    * public visible, to allow extra configuration if truely desired
    */
  val client = new AmazonDynamoDBClient(credentialsProvider, new ClientConfiguration().withMaxErrorRetry(0))


  def this(tableName: String, credentials: AWSCredentials) {
    this(tableName, new StaticCredentialsProvider(credentials))
  }


  // implement exponential backoff using Thread.sleep
  private def pause(retries: Int): Unit =
    if (retries > 0) {
      // 50, 100, 200, 400, 800
      val delay = (math.pow(2.0, (retries - 1).toDouble) * 50.0).toLong
      logger.debug(s"backing off for $delay msecs.")
      Thread.sleep(delay)
    }

  // write a batch, with infinite retry in the event of throttling
  private def writeWithBackoffRetry(batch: Batch): (Int, Double) = {
    var retries = 0
    var consumedCapacity = 0.0
    var batchRemaining = batch

    while (batchRemaining.size > 0) {
      // if retries > 0, pause
      pause(retries)
      retries += 1

      try {
        // build batch request
        val request = new BatchWriteItemRequest()
                        .withRequestItems(batchRemaining)
                        .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

        logger.debug(s"attempt $retries: writing ${batchRemaining.get(tableName).size} items to table $tableName in batch ${batchRemaining.hashCode}")

        // execute request
        val result = client.batchWriteItem(request)

        // retrieve consumed capacity
        val capacityConsumedByRequest = result.getConsumedCapacity().get(0).getCapacityUnits()
        logger.debug(s"consumed capacity: $capacityConsumedByRequest")
        consumedCapacity += capacityConsumedByRequest

        // retrieve any unprocessed items and use as the batch to continue with
        batchRemaining = result.getUnprocessedItems()
        if (batchRemaining.size > 0) {
          logger.debug(s"${batchRemaining.get(tableName).size} items were unprocessed in attempt $retries.")
        }
      } catch {
        case e: ProvisionedThroughputExceededException =>
          logger.debug(s"Caught ProvisionedThroughputExceededException for batchWriteItem with AWS Request ID: ${e.getRequestId()}")
          // continue in while loop, so current batchRemaining will be retried.
      }
    }

    // return the number of attempts required, along with the sum of the consumed capacity
    (retries, consumedCapacity)
  }


  // helper to make a write request batch
  private def mkBatch(requests: ju.List[WriteRequest]): Batch = {
    val m = new ju.HashMap[String, ju.List[WriteRequest]](1)
    m.put(tableName, requests)
    m
  }


  /**
    * Using synchronous calls in a single thread, write the
    * WriteRequest objects from the source Iterator in batches
    * using batchWriteItem.
    */
  def run(source: Iterator[WriteRequest]): Unit = {
    val rnd = new Random()
    // number of msecs by which to increase the pause
    val pauseIncrement = 25

    // starting batch size
    var batchSize = 2
    // starting pause duration
    var pauseDuration = pauseIncrement

    // records the system time of the completetion of the last successful write
    var startOfLastSuccessfulWrite = System.nanoTime()

    // 'slideing' average of estimated throughput
    var averagedThroughput = 0.0

    // records the system time of the start of the sample of writes
    var sampleStartTime = startOfLastSuccessfulWrite
    // number of samples taken
    var numOfSamplesTaken = 0
    // sum of capacity consumed by samples
    var consumedCapacitySampleSum = 0.0

    while (source.hasNext) {
      logger.debug(s"pausing for $pauseDuration msecs.")
      Thread.sleep(pauseDuration.toLong)

      // construct a batch of write requests
      val writeRequests = new ju.ArrayList[WriteRequest](batchSize)
      var i = 0
      while (source.hasNext && i < batchSize) {
        writeRequests.add(source.next())
        i += 1
      }
      val batch: Batch = mkBatch(writeRequests)

      // write the batch,
      // returning the number of attempts and the consumed capacity
      val (attempts, consumedCapacity) =
        try {
          writeWithBackoffRetry(batch)
        } catch {
          case e: AmazonServiceException if RetryUtils.isRequestEntityTooLargeException(e) =>
            // if request exceeded the 1Mb request limit
            val requests = batch.get(tableName)
            val size = requests.size
            val mid  = size / 2

            logger.warn(s"batch exceeded the request size limit! $batch")

            // split it into two and repeat seperately
            val (f_attempts, f_consumed) = writeWithBackoffRetry(mkBatch(requests.subList(0, mid)))
            val (s_attempts, s_consumed) = writeWithBackoffRetry(mkBatch(requests.subList(mid, size)))

            ( // return the max of the attempts, and sum of consumed capacity
              math.max(f_attempts, s_attempts),
              f_consumed + s_consumed
            )
        }

      // take system time after successfull batch write
      val afterSuccessfulWrite = System.nanoTime()

      // compute elapased seconds from last successful batch write to this successful batch write
      val estimatedTime = (afterSuccessfulWrite - startOfLastSuccessfulWrite) / 1000000000.0
      // reset last successful write start time
      startOfLastSuccessfulWrite = afterSuccessfulWrite

      // estimated throughput is capacity consumed by request, over total elapsed time (incl. pause)
      val estimatedThroughput = consumedCapacity / estimatedTime
      // sliding average takes a weighted average of current average and new observation
      averagedThroughput = (0.9 * averagedThroughput) + (0.1 * estimatedThroughput)

      // increment sample counter, modulo sample period
      numOfSamplesTaken = (numOfSamplesTaken + 1) % 10
      // add consumed capacity to sample sum
      consumedCapacitySampleSum += consumedCapacity

      if (numOfSamplesTaken == 0) {
        // compute elapsed seconds of the sample period
        val estimatedSampleTime = (afterSuccessfulWrite - sampleStartTime) / 1000000000.0
        // reset sample start time
        sampleStartTime = afterSuccessfulWrite

        // sample throughput is the sume of the sampled capacity, over elapsed sample time
        val sampleThroughput = consumedCapacitySampleSum / estimatedSampleTime
        // reset sample capacity
        consumedCapacitySampleSum = 0.0

        logger.debug(s"throughput sampled over last 10 batches is $sampleThroughput")
      }

      logger.debug(s"$attempts attempts required to write batch ${batch.hashCode}, consumed $consumedCapacity capacity, estimated throughput $estimatedThroughput, average throughput $averagedThroughput")
      if (attempts > 1) {
        // if backoff was required
        if (pauseDuration > 500 || rnd.nextDouble < 0.05) {
          // and pause duration is more than 1/2 a second, or randomly 5% of the time, decrease batchSize
          batchSize = math.max(2, batchSize - 1)
          logger.debug(s"decreasing batch size to $batchSize")
        } else {
          // else increase pause duration by the pause increment
          pauseDuration += pauseIncrement
          logger.debug(s"increasing pause by $pauseIncrement")
        }
      } else {
        // else no retries were needed so, reduce pause by 1 msec
        pauseDuration = math.max(0, pauseDuration - 1)

        if (pauseDuration == 0 || rnd.nextDouble < 0.05) {
          // if pause duration has hit zero, or randomly 5% of the time, increase batch size
          batchSize = math.min(25, batchSize + 1)
          logger.debug(s"increasing batch size to $batchSize")
        }
      }
    }
  }

}

// exceptions to watch out for
// ProvisionedThroughputExceededException: Status Code: 400, AWS Service: AmazonDynamoDBv2, AWS Request ID: 328QHMA32DO3L1M9K6O009CSSVVV4KQNSO5AEMVJF66Q9ASUAAJG, AWS Error Code: ProvisionedThroughputExceededException, AWS Error Message: The level of configured provisioned throughput for the table was exceeded. Consider increasing your provisioning level with the UpdateTable API
// AmazonServiceException: Status Code: 413, AWS Service: AmazonDynamoDBv2, AWS Request ID: null, AWS Error Code: Request entity too large, AWS Error Message: Request entity too large
// AmazonServiceException: Status Code: 400, AWS Service: AmazonDynamoDBv2, AWS Request ID: JGSD873F7DMDPG3L1A5DAS6CEFVV4KQNSO5AEMVJF66Q9ASUAAJG, AWS Error Code: ValidationException, AWS Error Message: Item size has exceeded the maximum allowed size of 65536
