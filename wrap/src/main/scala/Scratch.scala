
package aws.wrap

import simpledb.AmazonSimpleDBScalaClient

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.simpledb._
import com.amazonaws.services.simpledb.model._

object Scratch {

  val credentials = new AWSCredentials {
    override lazy val getAWSAccessKeyId =
      scala.io.Source.fromFile(System.getProperty("user.home") + "/.awssecret").getLines.toList(0)
    override lazy val getAWSSecretKey =
      scala.io.Source.fromFile(System.getProperty("user.home") + "/.awssecret").getLines.toList(1)
  }

  def main(args: Array[String]) {
    val client = AmazonSimpleDBScalaClient.fromAsyncClient(new AmazonSimpleDBAsyncClient(credentials))

    val future = for {
      _       <- client.createDomain("test")
      domains <- client.listDomains()
      _       <- client.deleteDomain("test")
    } yield domains

    val result = Await.result(future, Duration(10, SECONDS))
    println(result)

    client.shutdown()
  }
}
