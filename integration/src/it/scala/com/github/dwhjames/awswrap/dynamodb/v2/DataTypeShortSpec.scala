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

class DataTypeShortSpec
  extends FlatSpec
       with Matchers
       with DynamoDBClient
  {

    import DataTypeShortSpec._

    override val tableNames = Seq(DataTypeShort.tableName)

    val mapper = AmazonDynamoDBScalaMapper(client)

    override def beforeAll(): Unit = {
      super.beforeAll()

      tryCreateTable(DataTypeShort.tableRequest)
      awaitTableCreation(DataTypeShort.tableName)

      await(30.seconds) {
        mapper.batchDump(sampleData)
      }
    }

    "DynamoDB" should s"contain the '${DataTypeShort.tableName}' table" in {
      val result = await(1.minutes) {
        client.listTables()
      }

      result.getTableNames().asScala should contain (DataTypeShort.tableName)
    }

    it should "contain the first sample Item" in {
      await {
        mapper.loadByKey[DataTypeShort](sampleData.head.key)
      } .get should be (sampleData.head)

      val result = await {
        mapper.batchLoadByKeys[DataTypeShort](Seq(sampleData.head.key))
      }
      result should have size (1)
      result.head should be (sampleData.head)
    }

    it should s"contain ${sampleData.size} items" in {
      await {
        mapper.countScan[DataTypeShort]()
      } should equal (sampleData.size)
    }

    it should s"contain the sample items" in {
      val scan = await {
        mapper.scan[DataTypeShort]()
      }
      val scanOnce = await {
        mapper.scanOnce[DataTypeShort]()
      }
      val scanOnceLimit = await {
        mapper.scanOnce[DataTypeShort](limit = sampleData.size)
      }

      val progressively = await {
        mapper.scanProgressively[DataTypeShort](limit = sampleData.size).map(_._2)
      }

      val progressivelyLimit = await {
        for {
          (lastEvaluatedKey, head) <- mapper.scanProgressively[DataTypeShort](limit = 1)
          (lastEvaluatedKeyNone, next) <- mapper.scanProgressively[DataTypeShort](lastEvaluatedKey = lastEvaluatedKey)
        } yield {
          lastEvaluatedKey should not be empty
          lastEvaluatedKeyNone shouldEqual None
          head ++ next
        }
      }

      val batch = await {
        mapper.batchLoadByKeys[DataTypeShort](sampleData map (_.key))
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

object DataTypeShortSpec {
  val sampleData: Seq[DataTypeShort] = Seq(
    DataTypeShort(
      key = Short.MaxValue,
      set = Set(Short.MaxValue,Short.MinValue),
      list = List(Short.MaxValue,Short.MaxValue,Short.MinValue,Short.MinValue),
      map = Map("one" -> Short.MaxValue, "two" -> Short.MinValue)
    ),
    DataTypeShort(
      key = Short.MinValue,
      set = Set(Short.MinValue,Short.MaxValue),
      list = List(Short.MinValue,Short.MinValue,Short.MaxValue,Short.MaxValue),
      map = Map("one" -> Short.MinValue, "two" -> Short.MaxValue)
    )
  )
}
