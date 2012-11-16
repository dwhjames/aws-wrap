package aws.cloudsearch

import scala.annotation.implicitNotFound

import aws.core.AWSRegion

@implicitNotFound("You need to import a region to specify which datacenter you want to use.")
trait CloudSearchRegion extends AWSRegion

object CloudSearchRegion {

  private val NAME = "cloudsearch"

  import AWSRegion._

  val US_EAST_1 = new AWSRegion(US_EAST_1_NAME, US_EAST_1_DOMAIN, NAME) with CloudSearchRegion

  implicit val DEFAULT = US_EAST_1

}
