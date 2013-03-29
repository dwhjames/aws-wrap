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

import org.apache.commons.codec.binary.Hex

object Binary {

  /**
   * Converts byte data to a Hex-encoded string.
   *
   * @param bytes byte array to hex encode.
   * @return hex-encoded string.
   */
  def toHex(bytes: Array[Byte]): String =
    Hex.encodeHexString(bytes)

  /**
   * Converts a Hex-encoded data string to the original byte data.
   *
   * @param hexStr
   *            hex-encoded data to decode.
   * @return decoded data from the hex string.
   */
  def fromHex(hexStr: String): Array[Byte] =
    Hex.decodeHex(hexStr.toCharArray())

}
