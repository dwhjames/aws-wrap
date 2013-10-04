package com.pellucid.wrap.dynamodb

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

trait AwaitHelper {

  def await[T](atMost: Duration)(awaitable: Awaitable[T]): T =
    Await.result(awaitable, atMost)

  def await[T](awaitable: Awaitable[T]): T =
    Await.result(awaitable, 10.seconds)
}
