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

object Binary {

  /**
   * Converts byte data to a Hex-encoded string.
   *
   * @param data
   *            data to hex encode.
   *
   * @return hex-encoded string.
   */
  def toHex(data: Array[Byte]): String = {
    data.toSeq.map { byte =>
      Integer.toHexString(byte) match {
        case hex if hex.length == 1 => "0" + hex
        case hex if hex.length == 8 => hex.drop(6)
        case hex => hex
      }
    }.mkString("").toLowerCase
  }

  /**
   * Converts a Hex-encoded data string to the original byte data.
   *
   * @param hexData
   *            hex-encoded data to decode.
   * @return decoded data from the hex string.
   */
  def fromHex(hexData: String): Array[Byte] = {
    hexData.grouped(2).map(Integer.parseInt(_, 16).asInstanceOf[Byte]).toArray
  }

}
