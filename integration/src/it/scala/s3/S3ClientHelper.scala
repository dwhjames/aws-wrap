/*
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

package com.github.dwhjames.awswrap
package s3

import org.scalatest.{Suite, BeforeAndAfterAll}

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3._
import com.amazonaws.services.s3.transfer._

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


trait S3ClientHelper
  extends BeforeAndAfterAll
     with AwaitHelper
{ self: Suite =>

  private val logger: Logger = LoggerFactory.getLogger(self.getClass)

  val client = {
    val c = new AmazonS3ScalaClient(new BasicAWSCredentials("FAKE_ACCESS_KEY", "FAKE_SECRET_KEY"))
    c.client.setEndpoint("http://localhost:4000")
    c.client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true))
    c
  }

  val transferManager = new TransferManager(client.client)

  val bucketNames: Seq[String]

  override def beforeAll(): Unit = {
    bucketNames foreach { name =>
      logger.info(s"Creating bucket $name")
      client.client.createBucket(name)
    }

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
      bucketNames foreach { name =>
        logger.info(s"Deleting bucket $name")
        client.client.deleteBucket(name)
      }
    } finally {
      transferManager.shutdownNow()
      client.shutdown()
    }
  }
}
