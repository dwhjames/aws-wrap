package aws.s3

import aws.core.AWSRegion

object S3Region {

  val NAME = "s3"

  val US_EAST_1 = AWSRegion.US_EAST_1(NAME)
  val US_WEST_1 = AWSRegion.US_WEST_1(NAME)
  val US_WEST_2 = AWSRegion.US_WEST_2(NAME)
  val EU_WEST_1 = AWSRegion.EU_WEST_1(NAME)
  val SA_EAST_1 = AWSRegion.SA_EAST_1(NAME)

  implicit val DEFAULT = US_EAST_1
}
