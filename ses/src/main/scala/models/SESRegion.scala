package aws.ses

import scala.annotation.implicitNotFound

import aws.core.AWSRegion

@implicitNotFound("You need to import a region to specify which datacenter you want to use.")
trait SESRegion extends AWSRegion

object SESRegion {

  private val NAME = "ses"

  import AWSRegion._

  val US_EAST_1 = new AWSRegion(US_EAST_1_NAME, US_EAST_1_DOMAIN, NAME) with SESRegion

  implicit val DEFAULT = US_EAST_1

}
