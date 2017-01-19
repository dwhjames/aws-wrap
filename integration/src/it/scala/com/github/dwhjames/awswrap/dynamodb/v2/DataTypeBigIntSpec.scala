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

class DataTypeBigIntSpec
  extends FlatSpec
       with Matchers
       with DynamoDBClient
  {

    import DataTypeBigIntSpec._

    override val tableNames = Seq(DataTypeBigInt.tableName)

    val mapper = AmazonDynamoDBScalaMapper(client)

    override def beforeAll(): Unit = {
      super.beforeAll()

      tryCreateTable(DataTypeBigInt.tableRequest)
      awaitTableCreation(DataTypeBigInt.tableName)

      await(30.seconds) {
        mapper.batchDump(sampleData)
      }
    }

    "DynamoDB" should s"contain the '${DataTypeBigInt.tableName}' table" in {
      val result = await(1.minutes) {
        client.listTables()
      }

      result.getTableNames().asScala should contain (DataTypeBigInt.tableName)
    }

    it should "contain the first sample Item" in {
      await {
        mapper.loadByKey[DataTypeBigInt](sampleData.head.key)
      } .get should be (sampleData.head)

      val result = await {
        mapper.batchLoadByKeys[DataTypeBigInt](Seq(sampleData.head.key))
      }
      result should have size (1)
      result.head should be (sampleData.head)
    }

    it should s"contain ${sampleData.size} items" in {
      await {
        mapper.countScan[DataTypeBigInt]()
      } should equal (sampleData.size)
    }

    it should s"contain the sample items" in {
      val scan = await {
        mapper.scan[DataTypeBigInt]()
      }
      val scanOnce = await {
        mapper.scanOnce[DataTypeBigInt]()
      }
      val scanOnceLimit = await {
        mapper.scanOnce[DataTypeBigInt](limit = sampleData.size)
      }

      val progressively = await {
        mapper.scanProgressively[DataTypeBigInt](limit = sampleData.size).map(_._2)
      }

      val progressivelyLimit = await {
        for {
          (lastEvaluatedKey, head) <- mapper.scanProgressively[DataTypeBigInt](limit = 1)
          (lastEvaluatedKeyNone, next) <- mapper.scanProgressively[DataTypeBigInt](lastEvaluatedKey = lastEvaluatedKey)
        } yield {
          lastEvaluatedKey should not be empty
          lastEvaluatedKeyNone shouldEqual None
          head ++ next
        }
      }

      val batch = await {
        mapper.batchLoadByKeys[DataTypeBigInt](sampleData map (_.key))
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

object DataTypeBigIntSpec {
  val sampleData: Seq[DataTypeBigInt] = Seq(
    DataTypeBigInt(
      key = BigInt(1234567890123456789l),
      set = Set(BigInt(1),BigInt(2),BigInt(3)),
      list = List(BigInt(2),BigInt(2),BigInt(1),BigInt(3),BigInt(1)),
      map = Map("one" -> BigInt(1), "two" -> BigInt(2))
    ),
    DataTypeBigInt(
      key = BigInt(987654321098765432l),
      set = Set(BigInt(3),BigInt(2),BigInt(1)),
      list = List(BigInt(3),BigInt(3),BigInt(2),BigInt(1),BigInt(2)),
      map = Map("three" -> BigInt(3), "four" -> BigInt(4))
    )
  )
}
