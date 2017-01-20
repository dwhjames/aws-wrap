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

import java.io.File

import org.scalatest.{FlatSpec, Matchers}

class FutureTransferSpec
  extends FlatSpec
     with Matchers
     with S3ClientHelper
{

  val bucketName = "my-s3-bucket-98bfdf06-9475-4d1a-a235-e0eddd5859f9"
  override val bucketNames = Seq(bucketName)
  val objectKey = "test"

  override def afterAll(): Unit = {
    try {
      client.client.deleteObject(bucketName, objectKey)
    } finally {
      super.afterAll()
    }
  }

  "FutureTransfer" should "upload a file" in {
    val file = new File(
        this.getClass()
            .getClassLoader()
            .getResource("logback-test.xml")
            .toURI())

    val upload = transferManager.upload(bucketName, objectKey, file)

    await {
      FutureTransfer.listenFor(upload)
    }
    upload.waitForUploadResult()
    ()
  }

  it should "download a file" in {
    val file = File.createTempFile("logback-test", ".xml")

    try {
      val download = transferManager.download(bucketName, objectKey, file)

      await {
        FutureTransfer.listenFor(download)
      }
      download.waitForCompletion()
      ()
    } finally {
      file.delete()
      ()
    }
  }

}
