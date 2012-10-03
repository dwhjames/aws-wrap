package aws

import play.api.libs.ws._
import org.jboss.netty.handler.codec.base64.Base64

case class AWSignatureCalculator(accessKeyId: String, secretAccessKey: String) extends SignatureCalculator {

  override def sign(request: WS.WSRequest): Unit = {
    
  }

  private def signature(request: WS.WSRequest, extra: String = ""): String = {
    val tosign = request.method + "\n" +
      request.header("Content-Type").map(_ + "\n") +
      request.header("Date").map(_ + "\n") +
      extra
    HmacSha1.calculate(tosign, secretAccessKey)
  }

}


/*
 * 
Authorization = "AWS" + " " + AWSAccessKeyId + ":" + Signature;

Signature = Base64( HMAC-SHA1( YourSecretAccessKeyID, UTF-8-Encoding-Of( StringToSign ) ) );

StringToSign = HTTP-Verb + "\n" +
	Content-MD5 + "\n" +
	Content-Type + "\n" +
	Date + "\n" +
	CanonicalizedAmzHeaders +
	CanonicalizedResource;

CanonicalizedResource = [ "/" + Bucket ] +
	<HTTP-Request-URI, from the protocol name up to the query string> +
	[ sub-resource, if present. For example "?acl", "?location", "?logging", or "?torrent"];

CanonicalizedAmzHeaders = <described below>
*/
