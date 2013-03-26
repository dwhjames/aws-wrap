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

package aws.s3
package models

/**
 * Cross-Origin Resource Sharing rule
 * @see http://docs.amazonwebservices.com/AmazonS3/latest/dev/cors.html
 */
case class CORSRule(
  origins:       Seq[String]  = Nil,
  methods:       Seq[HttpMethod.Value]  = Nil,
  headers:       Seq[String]  = Nil,
  maxAge:        Option[Long] = None,
  exposeHeaders: Seq[String]  = Nil
)
