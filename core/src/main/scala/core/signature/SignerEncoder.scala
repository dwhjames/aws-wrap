package aws.core

import java.util.BitSet

/**
 * This encoder is to be used instead of the URLEncoder when encoding params for version 2 signing
 */
object SignerEncoder {

  private val dontEncode: BitSet = {
    // encode everything except what is included in the bitset
    val dontEncode = new BitSet(256)
    (Range.inclusive('a', 'z') ++ Range.inclusive('A', 'Z') ++ Range.inclusive('0', '9')).foreach(dontEncode.set(_))
    Seq('-', '_' , '.').foreach(dontEncode.set(_))
    dontEncode
  }

  def encode(str: String): String = {
    val lowerDiff = 'a' - 'A';
    str.getBytes("UTF-8").foldLeft("") { (r, c) => c match {
      case c if dontEncode.get(c) => r + c.toChar
      case c => {
        val ch0 = Character.forDigit((c.toChar >> 4) & 0xf, 16).toInt match {
          case letter if Character.isLetter(letter) => letter - lowerDiff
          case other => other
        }
        val ch1 = Character.forDigit(c.toChar & 0xf, 16).toInt match {
          case letter if Character.isLetter(letter) => letter - lowerDiff
          case other => other
        }
        r + "%" + ch0.toChar + ch1.toChar
      }
    }}
  }

}
