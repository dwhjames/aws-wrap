package aws.dynamodb

import scala.annotation.implicitNotFound

import aws.core.AWSRegion

@implicitNotFound("You need to import a region to specify which datacenter you want to use.")
trait DDBRegion extends AWSRegion

object DDBRegion {

  private val NAME = "dynamodb"

  import AWSRegion._

  val US_EAST_1 = new AWSRegion(US_EAST_1_NAME, US_EAST_1_DOMAIN, NAME) with DDBRegion
  val US_WEST_1 = new AWSRegion(US_WEST_1_NAME, US_WEST_1_DOMAIN, NAME) with DDBRegion
  val US_WEST_2 = new AWSRegion(US_WEST_2_NAME, US_WEST_2_DOMAIN, NAME) with DDBRegion
  val EU_WEST_1 = new AWSRegion(EU_WEST_1_NAME, EU_WEST_1_DOMAIN, NAME) with DDBRegion
  val ASIA_SOUTHEAST_1 = new AWSRegion(ASIA_SOUTHEAST_1_NAME, ASIA_SOUTHEAST_1_DOMAIN, NAME) with DDBRegion
  val ASIA_NORTHEAST_1 = new AWSRegion(ASIA_NORTHEAST_1_NAME, ASIA_NORTHEAST_1_DOMAIN, NAME) with DDBRegion

  implicit val DEFAULT = US_EAST_1

}
