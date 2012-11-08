/*
 * Copyright 2012 Pellucid and Zenexity
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

package aws.core

import scala.annotation.implicitNotFound

case class AWSRegion(name: String, subdomain: String, service: String) {
  def host = "%s.%s.%s".format(service, subdomain, AWSRegion.BASE)
}

object AWSRegion {

  val BASE = "amazonaws.com"

  val US_EAST_1_NAME = "US East (Northern Virginia) Region"
  val US_EAST_1_DOMAIN = "us-east-1"

  val US_WEST_1_NAME = "US West (Northern California) Region"
  val US_WEST_1_DOMAIN = "us-west-1"

  val US_WEST_2_NAME = "US West (Oregon) Region"
  val US_WEST_2_DOMAIN = "us-west-2"

  val EU_WEST_1_NAME = "EU (Ireland) Region"
  val EU_WEST_1_DOMAIN = "eu-west-1"

  val ASIA_SOUTHEAST_1_NAME = "Asia Pacific (Singapore) Region"
  val ASIA_SOUTHEAST_1_DOMAIN = "ap-southeast-1"

  val ASIA_NORTHEAST_1_NAME = "Asia Pacific (Tokyo) Region"
  val ASIA_NORTHEAST_1_DOMAIN = "ap-northeast-1"

  val SA_EAST_1_NAME = "South America (Sao Paulo) Region"
  val SA_EAST_1_DOMAIN = "sa-east-1"

}
