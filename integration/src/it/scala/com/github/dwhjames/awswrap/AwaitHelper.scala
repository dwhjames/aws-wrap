/*
 * Copyright 2012-2015 Pellucid Analytics
 * Copyright 2015 Daniel W. H. James
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

package com.github.dwhjames.awswrap

import scala.concurrent._
import scala.concurrent.duration._

private[awswrap] trait AwaitHelper {

  def await[T](atMost: Duration)(awaitable: Awaitable[T]): T =
    Await.result(awaitable, atMost)

  def await[T](awaitable: Awaitable[T]): T =
    Await.result(awaitable, 10.seconds)
}
