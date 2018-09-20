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

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import org.scalatest.{ FlatSpec, BeforeAndAfterAll, Matchers }

import com.amazonaws.AmazonClientException


class ReadsOnHashKeyTableSpec
  extends FlatSpec
     with Matchers
     with DynamoDBClient
{
  import SampleData.sampleForums

  override val tableNames = Seq(Forum.tableName)

  val mapper = AmazonDynamoDBScalaMapper(client)

  override def beforeAll(): Unit = {
    super.beforeAll()

    tryCreateTable(Forum.tableRequest)
    awaitTableCreation(Forum.tableName)

    await(30.seconds) {
      mapper.batchDump(sampleForums)
    }
  }

  "DynamoDB" should s"contain the '${Forum.tableName}' table" in {
    val result = await(1.minutes) {
      client.listTables()
    }

    result.getTableNames().asScala should contain (Forum.tableName)
  }

  it should "contain the first sample Forum" in {
    import org.scalatest.OptionValues._
    await {
      mapper.loadByKey[Forum](sampleForums.head.name)
    } .value should be (sampleForums.head)

    val result = await {
      mapper.batchLoadByKeys[Forum](Seq(sampleForums.head.name))
    }
    result should have size (1)
    result.head should be (sampleForums.head)
  }

  it should s"contain ${sampleForums.size} forum items" in {
    await {
      mapper.countScan[Forum]()
    } should equal (sampleForums.size)
  }

  it should s"contain the sample forum items" in {
    val forumScan = await {
      mapper.scan[Forum]()
    }
    val forumScanOnce = await {
      mapper.scanOnce[Forum]()
    }
    val forumScanOnceLimit = await {
      mapper.scanOnce[Forum](limit = sampleForums.size)
    }

    val forumProgressively = await {
      mapper.scanProgressively[Forum](limit = sampleForums.size).map(_._2)
    }

    val forumProgressivelyLimit = await {
      for {
        (lastEvaluatedKey, head) <- mapper.scanProgressively[Forum](limit = 1)
        (lastEvaluatedKeyNone, next) <- mapper.scanProgressively[Forum](lastEvaluatedKey = lastEvaluatedKey)
      } yield {
        lastEvaluatedKey should not be empty
        lastEvaluatedKeyNone shouldEqual None
        head ++ next
      }
    }

    val forumBatch = await {
      mapper.batchLoadByKeys[Forum](sampleForums map (_.name))
    }

    forumScan          should have size (sampleForums.size.toLong)
    forumScanOnce      should have size (sampleForums.size.toLong)
    forumScanOnceLimit should have size (sampleForums.size.toLong)
    forumProgressively should have size (sampleForums.size.toLong)
    forumProgressivelyLimit should have size (sampleForums.size.toLong)
    forumBatch         should have size (sampleForums.size.toLong)

    for (forum <- sampleForums) {
      forumScan     should contain (forum)
      forumScanOnce should contain (forum)
      forumBatch    should contain (forum)
    }
  }

}
