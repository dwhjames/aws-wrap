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

package com.github.dwhjames.awswrap.dynamodb.v2

import com.github.dwhjames.awswrap.dynamodb.v2.DataTypeConversions._
import com.github.dwhjames.awswrap.dynamodb.{AmazonDynamoDBScalaMapper, DynamoDBClient}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class DataTypeIntSpec
  extends FlatSpec
       with Matchers
       with DynamoDBClient
  {

    import DataTypeIntSpec._

    override val tableNames = Seq(DataTypeInt.tableName)

    val mapper = AmazonDynamoDBScalaMapper(client)

    override def beforeAll(): Unit = {
      super.beforeAll()

      tryCreateTable(DataTypeInt.tableRequest)
      awaitTableCreation(DataTypeInt.tableName)

      await(30.seconds) {
        mapper.batchDump(sampleData)
      }
    }

    "DynamoDB" should s"contain the '${DataTypeInt.tableName}' table" in {
      val result = await(1.minutes) {
        client.listTables()
      }

      result.getTableNames().asScala should contain (DataTypeInt.tableName)
    }

    it should "contain the first sample Item" in {
      await {
        mapper.loadByKey[DataTypeInt](sampleData.head.key)
      } .get should be (sampleData.head)

      val result = await {
        mapper.batchLoadByKeys[DataTypeInt](Seq(sampleData.head.key))
      }
      result should have size (1)
      result.head should be (sampleData.head)
    }

    it should s"contain ${sampleData.size} items" in {
      await {
        mapper.countScan[DataTypeInt]()
      } should equal (sampleData.size)
    }

    it should s"contain the sample items" in {
      val scan = await {
        mapper.scan[DataTypeInt]()
      }
      val scanOnce = await {
        mapper.scanOnce[DataTypeInt]()
      }
      val scanOnceLimit = await {
        mapper.scanOnce[DataTypeInt](limit = sampleData.size)
      }

      val progressively = await {
        mapper.scanProgressively[DataTypeInt](limit = sampleData.size).map(_._2)
      }

      val progressivelyLimit = await {
        for {
          (lastEvaluatedKey, head) <- mapper.scanProgressively[DataTypeInt](limit = 1)
          (lastEvaluatedKeyNone, next) <- mapper.scanProgressively[DataTypeInt](lastEvaluatedKey = lastEvaluatedKey)
        } yield {
          lastEvaluatedKey should not be empty
          lastEvaluatedKeyNone shouldEqual None
          head ++ next
        }
      }

      val batch = await {
        mapper.batchLoadByKeys[DataTypeInt](sampleData map (_.key))
      }

      scan          should have size (sampleData.size.toLong)
      scanOnce      should have size (sampleData.size.toLong)
      scanOnceLimit should have size (sampleData.size.toLong)
      progressively should have size (sampleData.size.toLong)
      progressivelyLimit should have size (sampleData.size.toLong)
      batch         should have size (sampleData.size.toLong)

      for (dataTypeV <- sampleData) {
        scan     should contain (dataTypeV)
        scanOnce should contain (dataTypeV)
        batch    should contain (dataTypeV)
      }
    }

  }

object DataTypeIntSpec {
  val sampleData: Seq[DataTypeInt] = Seq(
    DataTypeInt(
      key = 1,
      set = Set(1,2,3),
      list = List(2,2,1,3,1),
      map = Map("one" -> 1, "two" -> 2)
    ),
    DataTypeInt(
      key = 2,
      set = Set(3,2,1),
      list = List(3,3,2,1,2),
      map = Map("three" -> 3, "four" -> 4)
    )
  )
}
