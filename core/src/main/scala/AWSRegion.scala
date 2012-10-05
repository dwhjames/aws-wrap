package aws.core

import scala.annotation.implicitNotFound

@implicitNotFound(
  "You need to import a region to specify which datacenter you want to use."
)
case class AWSRegion(name: String, subdomain: String, service: String) {
  def host = "%s.%s.%s".format(service, subdomain, AWSRegion.BASE)
}

object AWSRegion {

  val BASE = "amazonaws.com"

  def US_EAST_1(service: String) = AWSRegion("US East (Northern Virginia) Region", "us-east-1", service)
  def US_WEST_1(service: String) = AWSRegion("US West (Northern California) Region", "us-west-1", service)
  def US_WEST_2(service: String) = AWSRegion("US West (Oregon) Region", "us-west-2", service)
  def EU_WEST_1(service: String) = AWSRegion("EU (Ireland) Region", "eu-west-1", service)
  def ASIA_SOUTHEAST_1(service: String) = AWSRegion("Asia Pacific (Singapore) Region", "ap-southeast-1", service)
  def ASIA_NORTHEAST_1(service: String) = AWSRegion("Asia Pacific (Tokyo) Region", "ap-northeast-1", service)
  def SA_EAST_1(service: String) = AWSRegion("South America (Sao Paulo) Region", "sa-east-1", service)

}
