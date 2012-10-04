package aws.core

import java.security.SignatureException
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

object HmacSha1 {
  val HMAC_SHA1_ALGORITHM = "HmacSHA1";

  def calculate(data: String, key: String): String = {
    val signingKey = new SecretKeySpec(key.getBytes(), HMAC_SHA1_ALGORITHM)
    val mac = Mac.getInstance(HMAC_SHA1_ALGORITHM)
    mac.init(signingKey)
    val rawHmac = mac.doFinal(data.getBytes())
    new sun.misc.BASE64Encoder().encode(rawHmac)
  }

}
