package aws.dynamodb

import scala.annotation.implicitNotFound

import aws.core.Region

case class DDBRegion(name: String, host: String)

@implicitNotFound(
  "You need to import a region to specify which datacenter you want to use. If you don't care, just import aws.simpledb.SDBRegion.DEFAULT"
)
object DDBRegion {

  def apply(region: Region) = new DDBRegion(region.name, "dynamodb." + region.host)

  implicit val DEFAULT = DDBRegion(Region.DEFAULT)
  val US_EAST_1 = DDBRegion(Region.US_EAST_1)
  val US_WEST_1 = DDBRegion(Region.US_WEST_1)
  val US_WEST_2 = DDBRegion(Region.US_WEST_2)
  val EU_WEST_1 = DDBRegion(Region.EU_WEST_1)
  val ASIA_SOUTHEAST_1 = DDBRegion(Region.ASIA_SOUTHEAST_1)
  val ASIA_NORTHEAST_1 = DDBRegion(Region.ASIA_NORTHEAST_1)

}
