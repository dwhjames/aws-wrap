package aws.s3.signature

import java.util.Date

import aws.core._
import aws.core.utils._

object S3Sign {

  val VERSION = "2009-04-15"
  val SIGVERSION = "2"
  val SIGMETHOD = "HmacSHA1"

  def dateFormat(d: Date) = new java.text.SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z").format(d)

  def canonicalizedResource(bucketName: Option[String], resource: Option[String] = None, subresource: Option[String] = None) =
    "%s%s%s".format(bucketName.map("/" + _.toLowerCase).getOrElse(""), resource.getOrElse("/"), subresource.map("?" + _).getOrElse(""))

  def canonicalizedAmzHeaders(headers: Seq[(String, String)]) = {
    headers match {
      case Nil => ""
      case _ => headers.map {
        case (k, v) =>
          k.toLowerCase -> v
        }.sortBy(_._1).map { case (k, v) => k + ":" + v }.mkString("\n") + "\n"
    }
  }

  def toSign(method: String, md5: String, contentType: String, date: String, amzheaders: String, resources: String) =
    "%s\n%s\n%s\n%s\n%s%s".format(method, md5, contentType, date, amzheaders, resources)

  def sign(method: String, bucketname: Option[String], subresource: Option[String], md5: Option[String] = None, contentType: Option[String] = None, headers: Seq[(String, String)] = Nil): Seq[(String, String)] = {

    import AWS.Parameters._
    import aws.core.SignerEncoder.encode

    val d = dateFormat(new Date)

    val s = toSign(
      method,
      md5.getOrElse(""),
      contentType.getOrElse(""),
      d,
      canonicalizedAmzHeaders(headers),
      canonicalizedResource(bucketname, None, subresource))

    ("Authorization" -> ("AWS " + AWS.key + ":" + signature(s))) :: ("Date" -> d) :: Nil
  }

  private def signature(data: String) = Crypto.base64(Crypto.hmacSHA1(data.getBytes(), AWS.secret))

}

