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

class DataTypeBigDecimalSpec
  extends FlatSpec
         with Matchers
         with DynamoDBClient
    {

      import DataTypeBigDecimalSpec._

      override val tableNames = Seq(DataTypeBigDecimal.tableName)

      val mapper = AmazonDynamoDBScalaMapper(client)

      override def beforeAll(): Unit = {
        super.beforeAll()

        tryCreateTable(DataTypeBigDecimal.tableRequest)
        awaitTableCreation(DataTypeBigDecimal.tableName)

        await(30.seconds) {
          mapper.batchDump(sampleData)
        }
      }

      "DynamoDB" should s"contain the '${DataTypeBigDecimal.tableName}' table" in {
        val result = await(1.minutes) {
          client.listTables()
        }

        result.getTableNames().asScala should contain (DataTypeBigDecimal.tableName)
      }

      it should "contain the first sample Item" in {
        await {
          mapper.loadByKey[DataTypeBigDecimal](sampleData.head.key)
        } .get should be (sampleData.head)

        val result = await {
          mapper.batchLoadByKeys[DataTypeBigDecimal](Seq(sampleData.head.key))
        }
        result should have size (1)
        result.head should be (sampleData.head)
      }

      it should s"contain ${sampleData.size} items" in {
        await {
          mapper.countScan[DataTypeBigDecimal]()
        } should equal (sampleData.size)
      }

      it should s"contain the sample items" in {
        val scan = await {
          mapper.scan[DataTypeBigDecimal]()
        }
        val scanOnce = await {
          mapper.scanOnce[DataTypeBigDecimal]()
        }
        val scanOnceLimit = await {
          mapper.scanOnce[DataTypeBigDecimal](limit = sampleData.size)
        }

        val progressively = await {
          mapper.scanProgressively[DataTypeBigDecimal](limit = sampleData.size).map(_._2)
        }

        val progressivelyLimit = await {
          for {
            (lastEvaluatedKey, head) <- mapper.scanProgressively[DataTypeBigDecimal](limit = 1)
            (lastEvaluatedKeyNone, next) <- mapper.scanProgressively[DataTypeBigDecimal](lastEvaluatedKey = lastEvaluatedKey)
          } yield {
            lastEvaluatedKey should not be empty
            lastEvaluatedKeyNone shouldEqual None
            head ++ next
          }
        }

        val batch = await {
          mapper.batchLoadByKeys[DataTypeBigDecimal](sampleData map (_.key))
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

object DataTypeBigDecimalSpec {
  val sampleData: Seq[DataTypeBigDecimal] = Seq(
    DataTypeBigDecimal(
      key = BigDecimal(1234567890123456789l),
      set = Set(BigDecimal(1),BigDecimal(2),BigDecimal(3)),
      list = List(BigDecimal(2),BigDecimal(2),BigDecimal(1),BigDecimal(3),BigDecimal(1)),
      map = Map("one" -> BigDecimal(1), "two" -> BigDecimal(2))
    ),
    DataTypeBigDecimal(
      key = BigDecimal(987654321098765432l),
      set = Set(BigDecimal(3),BigDecimal(2),BigDecimal(1)),
      list = List(BigDecimal(3),BigDecimal(3),BigDecimal(2),BigDecimal(1),BigDecimal(2)),
      map = Map("three" -> BigDecimal(3), "four" -> BigDecimal(4))
    )
  )
}
