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

class DataTypeFloatSpec
  extends FlatSpec
       with Matchers
       with DynamoDBClient
  {

    import DataTypeFloatSpec._

    override val tableNames = Seq(DataTypeFloat.tableName)

    val mapper = AmazonDynamoDBScalaMapper(client)

    override def beforeAll(): Unit = {
      super.beforeAll()

      tryCreateTable(DataTypeFloat.tableRequest)
      awaitTableCreation(DataTypeFloat.tableName)

      await(30.seconds) {
        mapper.batchDump(sampleData)
      }
    }

    "DynamoDB" should s"contain the '${DataTypeFloat.tableName}' table" in {
      val result = await(1.minutes) {
        client.listTables()
      }

      result.getTableNames().asScala should contain (DataTypeFloat.tableName)
    }

    it should "contain the first sample Item" in {
      await {
        mapper.loadByKey[DataTypeFloat](sampleData.head.key)
      } .get should be (sampleData.head)

      val result = await {
        mapper.batchLoadByKeys[DataTypeFloat](Seq(sampleData.head.key))
      }
      result should have size (1)
      result.head should be (sampleData.head)
    }

    it should s"contain ${sampleData.size} items" in {
      await {
        mapper.countScan[DataTypeFloat]()
      } should equal (sampleData.size)
    }

    it should s"contain the sample items" in {
      val scan = await {
        mapper.scan[DataTypeFloat]()
      }
      val scanOnce = await {
        mapper.scanOnce[DataTypeFloat]()
      }
      val scanOnceLimit = await {
        mapper.scanOnce[DataTypeFloat](limit = sampleData.size)
      }

      val progressively = await {
        mapper.scanProgressively[DataTypeFloat](limit = sampleData.size).map(_._2)
      }

      val progressivelyLimit = await {
        for {
          (lastEvaluatedKey, head) <- mapper.scanProgressively[DataTypeFloat](limit = 1)
          (lastEvaluatedKeyNone, next) <- mapper.scanProgressively[DataTypeFloat](lastEvaluatedKey = lastEvaluatedKey)
        } yield {
          lastEvaluatedKey should not be empty
          lastEvaluatedKeyNone shouldEqual None
          head ++ next
        }
      }

      val batch = await {
        mapper.batchLoadByKeys[DataTypeFloat](sampleData map (_.key))
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

object DataTypeFloatSpec {
  val sampleData: Seq[DataTypeFloat] = Seq(
    DataTypeFloat(
      key = Float.MaxValue,
      set = Set(Float.MaxValue,Float.MinValue,Float.MinPositiveValue),
      list = List(Float.MaxValue,Float.MaxValue,Float.MinPositiveValue,Float.MinValue,Float.MinPositiveValue),
      map = Map("one" -> Float.MaxValue, "two" -> Float.MinValue)
    ),
    DataTypeFloat(
      key = Float.MinValue,
      set = Set(Float.MinValue,Float.MinPositiveValue,Float.MaxValue),
      list = List(Float.MinValue,Float.MinValue,Float.MinPositiveValue,Float.MaxValue,Float.MinPositiveValue),
      map = Map("one" -> Float.MinValue, "two" -> Float.MaxValue)
    )
  )
}
