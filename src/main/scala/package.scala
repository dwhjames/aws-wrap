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

package com.github.dwhjames

import scala.concurrent.{Future, Promise}
import java.util.concurrent.{Future => JFuture}

import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.handlers.AsyncHandler

package object awswrap {

  private def promiseToAsyncHandler[Request <: AmazonWebServiceRequest, Result](p: Promise[Result]) =
    new AsyncHandler[Request, Result] {
      override def onError(exception: Exception): Unit = { p.failure(exception); () }
      override def onSuccess(request: Request, result: Result): Unit = { p.success(result); () }
    }

  private def promiseToVoidAsyncHandler[Request <: AmazonWebServiceRequest](p: Promise[Unit]) =
    new AsyncHandler[Request, Void] {
      override def onError(exception: Exception): Unit = { p.failure(exception); () }
      override def onSuccess(request: Request, result: Void): Unit = { p.success(()); () }
    }

  @inline
  private[awswrap] def wrapAsyncMethod[Request <: AmazonWebServiceRequest, Result](
    f:       (Request, AsyncHandler[Request, Result]) => JFuture[Result],
    request: Request
  ): Future[Result] = {
    val p = Promise[Result]
    f(request, promiseToAsyncHandler(p))
    p.future
  }

  @inline
  private[awswrap] def wrapVoidAsyncMethod[Request <: AmazonWebServiceRequest](
    f:       (Request, AsyncHandler[Request, Void]) => JFuture[Void],
    request: Request
  ): Future[Unit] = {
    val p = Promise[Unit]
    f(request, promiseToVoidAsyncHandler(p))
    p.future
  }
}
