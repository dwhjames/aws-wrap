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

import utils.Crypto

import java.util.Date

trait V2SignModule {

  def signUrl(method: String, url: String, params: Seq[(String, String)]): String
}

trait AbstractV2SignLayer {
  val V2Sign: V2SignModule
}

trait V2SignLayer extends AbstractV2SignLayer with AbstractCredentialsLayer {

  val v2SignVersion: String = "2009-04-15"

  override object V2Sign extends V2SignModule {

    private val SIGVERSION = "2"
    private val SIGMETHOD  = "HmacSHA1"

    private def signature(data: String): String =
      Crypto.base64(Crypto.hmacSHA1(data.getBytes(), awsSecret))

    private def path(url: String): String =
      url.split("/").drop(3).mkString("/", "/", "")

    private def host(url: String) =
      url.split("/").drop(2).head

    override def signUrl(method: String, url: String, params: Seq[(String, String)]): String = {

      val ps = Seq(
        Parameters.TimeStamp(new Date),
        Parameters.AWSAccessKeyId(awsKey),
        Parameters.Version(v2SignVersion),
        Parameters.SignatureVersion(SIGVERSION),
        Parameters.SignatureMethod(SIGMETHOD)
      )

      val queryString = canonicalQueryString(params ++ ps)

      val toSign = s"$method\n${host(url)}\n${path(url)}\n$queryString"

      s"Signature=${SignerEncoder.encode(signature(toSign))}&$queryString"
    }
  }
}
