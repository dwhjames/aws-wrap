package aws.simpledb

import java.util.Date

import scala.util.{ Try, Success }
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import play.api.libs.ws._

import aws.core._
import aws.core.parsers._
import aws.simpledb.SDBParsers._

case class SDBRegion(region: String, endpoint: String)
case class SDBAttribute(name: String, value: String, replace: Option[Boolean] = None)
case class SDBDomain(name: String)

object SDBRegion {
  val US_EAST_1 = SDBRegion("US East (Northern Virginia) Region", "sdb.amazonaws.com")
  val US_WEST_1 = SDBRegion("US West (Northern California) Region", "sdb.us-west-1.amazonaws.com")
  val US_WEST_2 = SDBRegion("US West (Oregon) Region", "sdb.us-west-2.amazonaws.com")
  val EU_WEST_1 = SDBRegion("EU (Ireland) Region", "sdb.eu-west-1.amazonaws.com")
  val ASIA_SOUTHEAST_1 = SDBRegion("Asia Pacific (Singapore) Region", "sdb.ap-southeast-1.amazonaws.com")
  val ASIA_NORTHEAST_1 = SDBRegion("Asia Pacific (Tokyo) Region", "sdb.ap-northeast-1.amazonaws.com")
  val SA_EAST_1 = SDBRegion("South America (Sao Paulo) Region", "sdb.sa-east-1.amazonaws.com")

  implicit val DEFAULT = US_EAST_1
}

object SimpleDB {

  object Parameters {
    def DomainName(a: String) = ("DomainName" -> a)
    def ItemName(a: String) = ("ItemName" -> a)
    def MaxNumberOfDomains(n: Int) = ("MaxNumberOfDomains" -> n.toString)
    def NextToken(n: String) = ("NextToken" -> n)
    def Attributes(attrs: Seq[SDBAttribute]): Seq[(String, String)] = (for ((attribute, i) <- attrs.zipWithIndex) yield {
      Seq(
        "Attribute.%s.Name".format((i + 1).toString) -> attribute.name,
        "Attribute.%s.Value".format((i + 1).toString) -> attribute.value
      ) ++ {
          if (attribute.replace.isDefined) Seq("Attribute.%s.Replace".format((i + 1).toString) -> attribute.replace.get.toString) else Nil
        }
    }).flatten
  }

  import AWS.Parameters._
  import Parameters._
  import SDBRegion.DEFAULT

  val HOST = "sdb.amazonaws.com"

  private def request(parameters: (String, String)*): Future[Response] = {
    WS.url("https://" + HOST + "/?" + SimpleDBCalculator.url("GET", parameters)).get()
  }

  /**
   * Creates a domain with the given name
   */
  def createDomain(domainName: String)(implicit region: SDBRegion): Future[Try[Result]] = {
    request(Action("CreateDomain"), DomainName(domainName)).map { wsresponse =>
      aws.core.EmptyResult(wsresponse)
    }
  }

  /**
   * Deletes the given domain
   */
  def deleteDomain(domainName: String)(implicit region: SDBRegion): Future[Try[Result]] = {
    request(Action("DeleteDomain"), DomainName(domainName)).map { wsresponse =>
      aws.core.EmptyResult(wsresponse)
    }
  }

  /**
   * Lists domains starting with the nextToken, if present, and giving the max number of domains given
   */
  def listDomains(maxNumberOfDomains: Int = 100, nextToken: Option[String] = None)(implicit region: SDBRegion): Future[Try[SimpleResult[Seq[SDBDomain]]]] = {
    val params = Seq(
      Action("ListDomains"),
      MaxNumberOfDomains(maxNumberOfDomains)) ++ nextToken.map(NextToken(_)).toSeq

    request(params: _*).map { wsresponse =>
      SimpleResult(wsresponse.xml, Parser.of[Seq[SDBDomain]])
    }
  }

  /**
   * Puts the attributes into SimpleDB
   */
  def putAttributes(domainName: String, itemName: String, attributes: Seq[SDBAttribute])(implicit region: SDBRegion): Future[Try[EmptyResult]] = {
    val params = Seq(
      Action("ListDomains"),
      DomainName(domainName),
      ItemName(itemName)
    ) ++ Attributes(attributes)

    request(params: _*).map { wsresponse =>
      aws.core.EmptyResult(wsresponse)
    }
  }
  /*
    /**
     * Deletes an entire item
     **/
    def deleteAttributes(domainName: String, itemName: String)(implicit http: dispatch.Http, region: SimpleDBRegion = US_EAST_1): dispatch.Promise[(Response, Elem)] = {
        val params: Map[String, String] = Map(ACTION -> "DeleteAttributes", "ItemName" -> itemName, "DomainName" -> domainName)
                performRequest(host = region.endpoint, params = params, method = POST)
    }

    /**
     * Gets Attributes from the given domain name
     **/
    def getAttributes(domainName: String, itemName: String, attributeName: Option[String] = None, consistentRead: Option[Boolean] = None)(implicit http: dispatch.Http, region: SimpleDBRegion = US_EAST_1): dispatch.Promise[(Response, Elem)] = {
        val params: Map[String, String] = Map(ACTION -> "GetAttributes", "DomainName" -> domainName, "ItemName" -> itemName) ++
                {if(attributeName.isDefined) Map("AttributeName" -> attributeName.get) else Map[String, String]()} ++
                {if(consistentRead.isDefined) Map("ConsistentRead" -> consistentRead.get.toString) else Map[String, String]()}
                performRequest(host = region.endpoint, params = params)
    }
*/
}

object SimpleDBCalculator {

  val VERSION = "2009-04-15"
  val SIGVERSION = "2"
  val SIGMETHOD = "HmacSHA1"

  def url(method: String, params: Seq[(String, String)]): String = {

    import AWS.Parameters._
    import aws.core.SignerEncoder.encode

    val ps = Seq(
      Expires(600L),
      AWSAccessKeyId(AWS.key),
      Version(VERSION),
      SignatureVersion(SIGVERSION),
      SignatureMethod(SIGMETHOD)
    )

    val queryString = (params ++ ps).sortBy(_._1)
      .map { p => encode(p._1) + "=" + encode(p._2) }.mkString("&")

    val toSign = "%s\n%s\n%s\n%s".format(method, SimpleDB.HOST, "/", queryString)

    "Signature=" + encode(signature(toSign)) + "&" + queryString
  }

  def signature(data: String) = HmacSha1.calculate(data, AWS.secret)

}
