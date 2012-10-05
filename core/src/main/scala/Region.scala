package aws.core

case class Region(name: String, host: String)

object Region {
  val DEFAULT = Region("US East (Northern Virginia) Region", "amazonaws.com")
  val US_EAST_1 = Region("US East (Northern Virginia) Region", "us-east-1.amazonaws.com")
  val US_WEST_1 = Region("US West (Northern California) Region", "us-west-1.amazonaws.com")
  val US_WEST_2 = Region("US West (Oregon) Region", "us-west-2.amazonaws.com")
  val EU_WEST_1 = Region("EU (Ireland) Region", "eu-west-1.amazonaws.com")
  val ASIA_SOUTHEAST_1 = Region("Asia Pacific (Singapore) Region", "ap-southeast-1.amazonaws.com")
  val ASIA_NORTHEAST_1 = Region("Asia Pacific (Tokyo) Region", "ap-northeast-1.amazonaws.com")
  val SA_EAST_1 = Region("South America (Sao Paulo) Region", "sa-east-1.amazonaws.com")
}
