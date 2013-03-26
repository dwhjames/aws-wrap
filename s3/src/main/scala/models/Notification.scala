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

package aws.s3.models

import scala.concurrent.Future
import scala.xml.Node

import aws.core.Types.EmptyResult

import aws.s3.S3Parsers._

object Notification {
  object Events extends Enumeration {
    type Event = Value
    val REDUCED_REDUNDANCY_LOST_OBJECT = Value("s3:ReducedRedundancyLostObject")
  }
}

case class NotificationConfiguration(topic: String, event: Notification.Events.Event)
