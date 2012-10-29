package aws.core.utils

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

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

  def base64(data: Array[Byte]) = new sun.misc.BASE64Encoder().encode(data)

  def decodeBase64(b64: String) = new sun.misc.BASE64Decoder().decodeBuffer(b64)

}
