package aws.s3

import play.api.libs.ws._
import play.api.libs.ws.WS._
import aws.core._

object S3 {

  val ACCESS_KEY_ID = ""
  val SECRET_ACCESS_KEY = ""
  /*
  def createBucket(name: String) = {
      val calculator = AWSignatureCalculator(ACCESS_KEY_ID, SECRET_ACCESS_KEY)
      val body = <CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"> 
          <LocationConstraint>BucketRegion</LocationConstraint> 
          </CreateBucketConfiguration>
      val response = WS.url("/sdasdfasdf").sign(calculator).put(body).value.get
      println("Response: " + response.status)
      println(response.body)
  }
*/
}

