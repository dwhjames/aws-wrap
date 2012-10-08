package aws.core.signature

import aws.core._
import aws.core.utils._
import java.text.SimpleDateFormat
import java.util.Date

object V4 {

  def derivedKey(date: Option[Date] = None)(implicit region: AWSRegion): Array[Byte] = {
    val formattedDate = new SimpleDateFormat("YYYYmmdd").format(date.getOrElse(new Date()))
    val kDate = Crypto.hmacSHA256(("AWS4" + AWS.secret).getBytes(), formattedDate)
    val kRegion = Crypto.hmacSHA256(kDate, region.subdomain)
    val kService = Crypto.hmacSHA256(kRegion, region.service)
    Crypto.hmacSHA256(kService, "aws4_request")
  }

}

