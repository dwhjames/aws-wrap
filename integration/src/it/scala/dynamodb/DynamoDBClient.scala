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

package com.pellucid.wrap.dynamodb

import scala.collection.JavaConverters._
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import org.scalatest.{Suite, BeforeAndAfterAll}
import org.scalatest.matchers.ShouldMatchers

import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.services.dynamodbv2._
import com.amazonaws.services.dynamodbv2.model._

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


trait DynamoDBClient
  extends BeforeAndAfterAll
     with AwaitHelper
{ self: Suite =>

  private val logger: Logger = LoggerFactory.getLogger(self.getClass)

  val client = {
    val inputStream =
      this.getClass()
          .getClassLoader()
          .getResourceAsStream("credentials.properties")

    require(inputStream ne null, """
      |*A file called credentials.properties was not found on the classpath.*
      |
      |Please add this file and place your AWS credentials in it. The required
      |format is specified in the provided credentials-template.properties file.
      """.stripMargin)

    val credentials =
      try {
        new PropertiesCredentials(inputStream)
      } catch {
        case ex: IllegalArgumentException =>
          throw new IllegalArgumentException("""requirement failed:
            |*The credentials.properties file was not properly specified*
            |
            |The required format is specified in the provided
            |credentials-template.properties file.
            """.stripMargin)
      }

    val jClient = new AmazonDynamoDBAsyncClient(credentials)
    if (System.getProperty("DynamoDB.localMode") == "true")
      jClient.setEndpoint("http://localhost:8000")

    new AmazonDynamoDBScalaClient(jClient)
  }

  val tableNames: Seq[String]

  override def beforeAll() {
    deleteAllSpecifiedTables()

    super.beforeAll()
  }

  override def afterAll() {
    try {
      super.afterAll()
    } finally {
      deleteAllSpecifiedTables()
    }
  }

  private def deleteAllSpecifiedTables() {
    tableNames foreach tryDeleteTable

    tableNames foreach awaitTableDeletion
  }

  def tryDeleteTable(tableName: String) {
    logger.info(s"Deleting $tableName table")
    await {
      client.deleteTable(tableName) recover { case e: ResourceNotFoundException => () }
    }
  }

  def awaitTableDeletion(tableName: String) {
    logger.info(s"Waiting for $tableName table to be deleted.")

    val deadline = 10.minutes.fromNow

    while (deadline.hasTimeLeft) {
      try {
        val result = await {
          client.describeTable(tableName)
        }

        if (result.getTable.getTableStatus == TableStatus.ACTIVE.toString) return ()
        Thread.sleep(20 * 1000)
      } catch {
        case e: ResourceNotFoundException =>
          return ()
      }
    }
    throw new RuntimeException(s"Timed out waiting for $tableName table to be deleted.")
  }

  def tryCreateTable(createTableRequest: CreateTableRequest) {
    logger.info(s"Creating ${createTableRequest.getTableName()} table")
    await {
      client.createTable(createTableRequest)
    }
  }

  def awaitTableCreation(tableName: String): TableDescription = {
      logger.info(s"Waiting for $tableName table to become active.")

      val deadline = 10.minutes.fromNow

      while (deadline.hasTimeLeft) {
        val result = await {
          client.describeTable(tableName)
        }

        val description = result.getTable
        if (description.getTableStatus == TableStatus.ACTIVE.toString)
          return description

        Thread.sleep(20 * 1000)
      }
      throw new RuntimeException(s"Timed out waiting for $tableName table to become active.")
    }

}
