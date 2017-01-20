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

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class DataTypeConversionSpec
  extends FlatSpec
     with Matchers
     with DynamoDBClient
{

  import DataTypeConversionSpec._

  override val tableNames = Seq(DataTypeVarieties.tableName)

  val mapper = AmazonDynamoDBScalaMapper(client)

  override def beforeAll(): Unit = {
    super.beforeAll()

    tryCreateTable(DataTypeVarieties.tableRequest)
    awaitTableCreation(DataTypeVarieties.tableName)

    await(30.seconds) {
      mapper.batchDump(sampleData)
    }
  }

  "DynamoDB" should s"contain the '${DataTypeVarieties.tableName}' table" in {
    val result = await(1.minutes) {
      client.listTables()
    }

    result.getTableNames().asScala should contain (DataTypeVarieties.tableName)
  }

  it should "contain the first sample DataTypeV" in {
    await {
      mapper.loadByKey[DataTypeVarieties](sampleData.head.string)
    } .get should be (sampleData.head)

    val result = await {
      mapper.batchLoadByKeys[DataTypeVarieties](Seq(sampleData.head.string))
    }
    result should have size (1)
    result.head should be (sampleData.head)
  }

  it should s"contain ${sampleData.size} DataTypeV items" in {
    await {
      mapper.countScan[DataTypeVarieties]()
    } should equal (sampleData.size)
  }

  it should s"contain the sample DataTypeV items" in {
    val scan = await {
      mapper.scan[DataTypeVarieties]()
    }
    val scanOnce = await {
      mapper.scanOnce[DataTypeVarieties]()
    }
    val scanOnceLimit = await {
      mapper.scanOnce[DataTypeVarieties](limit = sampleData.size)
    }

    val progressively = await {
      mapper.scanProgressively[DataTypeVarieties](limit = sampleData.size).map(_._2)
    }

    val progressivelyLimit = await {
      for {
        (lastEvaluatedKey, head) <- mapper.scanProgressively[DataTypeVarieties](limit = 1)
        (lastEvaluatedKeyNone, next) <- mapper.scanProgressively[DataTypeVarieties](lastEvaluatedKey = lastEvaluatedKey)
      } yield {
        lastEvaluatedKey should not be empty
        lastEvaluatedKeyNone shouldEqual None
        head ++ next
      }
    }

    val batch = await {
      mapper.batchLoadByKeys[DataTypeVarieties](sampleData map (_.string))
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

object DataTypeConversionSpec {
  val sampleData: Seq[DataTypeVarieties] = Seq(
    DataTypeVarieties(
      string = "String 1",
      bigDecimal = BigDecimal(1234567890123456789l),
      bigInt = BigInt(987654321098765432l),
      double = 1.toDouble,
      float = Float.MinValue,
      long= 12.toLong,
      int = 13,
      byte = 11.toByte,
      short = 2.toShort,
      bool = true,
      strings = Set("one","two","three"),
      ints = Set(1,2,3),
      bytes = Set(Byte.MaxValue, Byte.MinValue)
    ),
    DataTypeVarieties(
      string = "String 2",
      bigDecimal = BigDecimal(987654321098765432l),
      bigInt = BigInt(1234567890123456789l),
      double = 2.toDouble,
      float = Float.MaxValue,
      long= 120.toLong,
      int = 130,
      byte = 14.toByte,
      short = 7.toShort,
      bool = false,
      strings = Set("three","two","one"),
      ints = Set(3,2,1),
      bytes = Set(Byte.MinValue, Byte.MaxValue)
    )
  )
}
