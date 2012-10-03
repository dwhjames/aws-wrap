package aws

import java.security.SignatureException
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import com.ning.http.util.Base64

object HmacSha1 {
  val HMAC_SHA1_ALGORITHM = "HmacSHA1";

  def calculate(data: String, key: String): String = {
    val signingKey = new SecretKeySpec(key.getBytes(), HMAC_SHA1_ALGORITHM)
    val mac = Mac.getInstance(HMAC_SHA1_ALGORITHM)
    mac.init(signingKey)
    val rawHmac = mac.doFinal(data.getBytes())
    Base64.encode(rawHmac)
  }

}
