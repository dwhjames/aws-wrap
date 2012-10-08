package aws.core.signature

import aws.core._
import aws.core.utils._
import java.text.SimpleDateFormat
import java.util.Date

object V4 {

  val VERSION = "20111205"

  val ALGO = "AWS4-HMAC-SHA256"

  def derivedKey(date: Option[Date] = None)(implicit region: AWSRegion): Array[Byte] = {
    val formattedDate = new SimpleDateFormat("yyyyMMdd").format(date.getOrElse(new Date()))
    val kDate = Crypto.hmacSHA256(formattedDate.getBytes, ("AWS4" + AWS.secret).getBytes())
    val kRegion = Crypto.hmacSHA256(region.subdomain.getBytes, kDate)
    val kService = Crypto.hmacSHA256(region.service.getBytes, kRegion)
    Crypto.hmacSHA256("aws4_request".getBytes, kService)
  }

  def canonicalHeaders(headers: Seq[(String, String)]) =
    headers.sortBy(_._1).map { h => h._1.toLowerCase + ":" + h._2.trim + "\n" }.mkString

  def signedHeaders(headers: Seq[(String, String)]) =
    headers.sortBy(_._1).map { h => h._1.toLowerCase }.mkString(";")

  def canonicalRequest(headers: Seq[(String, String)], queryString: Seq[(String, String)], body: String) = {
    "POST" + '\n' +
      "/" + '\n' +
      canonicalQueryString(queryString) + '\n' +
      canonicalHeaders(headers) + '\n' +
      signedHeaders(headers) + '\n' +
      Binary.toHex(hash(body))
  }

  /**
   * Hashes the string contents (assumed to be UTF-8) using the SHA-256
   * algorithm.
   *
   * @param text The string to hash.
   *
   * @return The hashed bytes from the specified string.
   */
  def hash(text: String): Array[Byte] = {
    import java.security.MessageDigest
    val md = MessageDigest.getInstance("SHA-256")
    md.update(text.getBytes("UTF-8"))
    md.digest()
  }

  def signature(requestTime: java.util.Date,
                hdrs: Seq[(String, String)],
                qs: Seq[(String, String)] = Nil,
                body: String = "")(implicit region: AWSRegion) = {
    val stringToSign =
      ALGO + '\n' +
        AWS.isoBasicFormat(requestTime) + '\n' +
        credentialScope + '\n' +
        Binary.toHex(V4.hash(V4.canonicalRequest(hdrs, qs, body)))
    Crypto.hmacSHA256(stringToSign.getBytes(), V4.derivedKey())
  }

  def credentialScope(implicit region: AWSRegion): String =
    new SimpleDateFormat("yyyyMMdd").format(new Date()) + "/" + region.subdomain + "/" + region.service + "/aws4_request"

  def authorizationHeader(requestTime: java.util.Date,
                          headers: Seq[(String, String)],
                          qs: Seq[(String, String)] = Nil,
                          body: String = "")(implicit region: AWSRegion) = {
    val credentialsAuthorizationHeader = "Credential=" + AWS.key + "/" + credentialScope;
    val signedHeadersAuthorizationHeader = "SignedHeaders=" + signedHeaders(headers);
    val signatureAuthorizationHeader = "Signature=" + Binary.toHex(signature(requestTime, headers, qs, body));

    ALGO + " " +
      credentialsAuthorizationHeader + ", " +
      signedHeadersAuthorizationHeader + ", " +
      signatureAuthorizationHeader
  }

}

