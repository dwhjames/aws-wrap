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
