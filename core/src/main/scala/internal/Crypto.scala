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

package aws.core.utils

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

import org.apache.commons.codec.binary.Base64

object Crypto {
  val HMAC_SHA1_ALGORITHM = "HmacSHA1";
  val HMAC_SHA256_ALGORITHM = "HmacSHA256";

  def hmacSHA1(data: Array[Byte], key: Array[Byte]): Array[Byte] = hmac(data, key, HMAC_SHA1_ALGORITHM)
  def hmacSHA1(data: Array[Byte], key: String): Array[Byte] = hmac(data, key.getBytes, HMAC_SHA1_ALGORITHM)

  def hmacSHA256(data: Array[Byte], key: Array[Byte]): Array[Byte] = hmac(data, key, HMAC_SHA256_ALGORITHM)
  def hmacSHA256(data: Array[Byte], key: String): Array[Byte] = hmac(data, key.getBytes, HMAC_SHA256_ALGORITHM)

  def hmac(data: Array[Byte], key: Array[Byte], algorithm: String): Array[Byte] = {
    val signingKey = new SecretKeySpec(key, algorithm)
    val mac = Mac.getInstance(algorithm)
    mac.init(signingKey)
    mac.doFinal(data)
  }

  def base64(data: Array[Byte]): String =
    Base64.encodeBase64String(data)

  def decodeBase64(b64: String): Array[Byte] =
    Base64.decodeBase64(b64)

}
