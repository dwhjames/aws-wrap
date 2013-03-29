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
 package modules

 import parsers.Parser

 import scala.concurrent.{ExecutionContext, Future}

 import play.api.libs.ws.{Response, WS}

 trait HttpRequestV2Module[M <: Metadata] {

  def get[T](parameters: (String, String)*)(implicit region: AWSRegion, p: Parser[Result[M, T]]): Future[Result[M, T]]

  def get[T](resource: String, parameters: (String, String)*)(implicit p: Parser[Result[M, T]]): Future[Result[M, T]]

 }

 trait AbstractHttpRequestV2Layer[M <: Metadata] {
  val Http: HttpRequestV2Module[M]
 }

 trait HttpRequestV2Layer[M <: Metadata] extends AbstractHttpRequestV2Layer[M] with AbstractSigV2Layer {

  protected implicit val v2RequestExecutionContext: ExecutionContext

  override object Http extends HttpRequestV2Module[M] {

    private def request(resource: String, parameters: Seq[(String, String)]): Future[Response] =
      WS.url(s"""${resource}?${SigV2.signUrl("GET", resource, parameters)}""").get()

    private def tryParse[T](resp: Response)(implicit p: Parser[Result[M, T]]): Result[M, T] =
      Parser.parse[Result[M, T]](resp).fold(
        e => throw new RuntimeException(e),
        identity
      )

    def get[T](parameters: (String, String)*)(implicit region: AWSRegion, p: Parser[Result[M, T]]): Future[Result[M, T]] =
      get[T](s"https://${region.host}/", parameters: _*)

    def get[T](resource: String, parameters: (String, String)*)(implicit p: Parser[Result[M, T]]): Future[Result[M, T]] =
      request(resource, parameters).map(tryParse[T])

  }
}
