package aws.simpledb

import java.util.Date

import scala.concurrent.Future
import play.api.libs.ws._
import play.api.libs.ws.WS._
import aws.core._

case class SimpleDBRegion(region: String, endpoint: String)

case class SimpleDBAttribute(name: String, value: String, replace: Option[Boolean] = None)

object SimpleDB {

  object Parameters {
    def DomainName(a: String) = ("DomainName" -> a)
  }

  object Regions {
    //The Different SimpleDBRegions
    val US_EAST_1 = SimpleDBRegion("US East (Northern Virginia) Region", "sdb.amazonaws.com")
    val US_WEST_1 = SimpleDBRegion("US West (Northern California) Region    ", "sdb.us-west-1.amazonaws.com")
    val US_WEST_2 = SimpleDBRegion("US West (Oregon) Region", "sdb.us-west-2.amazonaws.com")
    val EU_WEST_1 = SimpleDBRegion("EU (Ireland) Region", "sdb.eu-west-1.amazonaws.com")
    val ASIA_SOUTHEAST_1 = SimpleDBRegion("Asia Pacific (Singapore) Region", "sdb.ap-southeast-1.amazonaws.com")
    val ASIA_NORTHEAST_1 = SimpleDBRegion("Asia Pacific (Tokyo) Region", "sdb.ap-northeast-1.amazonaws.com")
    val SA_EAST_1 = SimpleDBRegion("South America (Sao Paulo) Region", "sdb.sa-east-1.amazonaws.com")

    implicit val DEFAULT = US_EAST_1
  }

  import AWS.Parameters._
  import Parameters._
  import Regions.DEFAULT

  val HOST = "sdb.amazonaws.com"

  private def request(parameters: (String, String)*): Future[Response] = {
    WS.url("https://" + HOST + "/?" + SimpleDBCalculator.url("GET", parameters)).get()
  }

  /**
   * Creates a domain with the given name
   */
  def createDomain(domainName: String)(implicit region: SimpleDBRegion): Future[Response] = {
    request(Action("CreateDomain"), DomainName(domainName), Expires(600L))
  }
  /*
  /**
   * Lists domains starting with the nextToken, if present, and giving the mas number of domains given
   **/
  def listDomains(maxNumberOfDomains: Int = 100, nextToken: Option[String] = None)(implicit region: SimpleDBRegion): Promise[(Response, Elem)] = {
    val params = Map(ACTION -> "ListDomains", "MaxNmberOfDomains" -> maxNumberOfDomains.toString) ++
      {if(nextToken.isDefined) Map("NextToken" -> nextToken.get) else Map[String, String]()}
      performRequest(host = region.endpoint, params = params)
  }


    /**
     * Deletes the given domain
     **/
    def deleteDomain(domainName: String)(implicit http: dispatch.Http, region: SimpleDBRegion = US_EAST_1): dispatch.Promise[(Response, Elem)] = {
        performRequest(host = region.endpoint, params = Map(ACTION -> "DeleteDomain", "DomainName" -> domainName))
    }

    /**
     * Puts the attributes into SimpleDB
     **/
    def putAttributes(domainName: String, itemName: String, attributes: Seq[SimpleDBAttribute])(implicit http: dispatch.Http, region: SimpleDBRegion = US_EAST_1): dispatch.Promise[(Response, Elem)] = {
        val params: Map[String, String] = Map(ACTION -> "PutAttributes", "ItemName" -> itemName, "DomainName" -> domainName) ++
                (for((attribute, i) <- attributes.zipWithIndex) yield {
                    Map(
                            "Attribute.%s.Name".format((i + 1).toString) -> attribute.name,
                            "Attribute.%s.Value".format((i + 1).toString) -> attribute.value
                            ) ++ {if(attribute.replace.isDefined) Map("Attribute.%s.Replace".format((i + 1).toString) -> attribute.replace.get.toString) else Map[String, String]()}
                }).foldLeft(Map[String, String]())(_ ++ _)
                performRequest(host = region.endpoint, params = params, method = POST)
    }

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
    import SignerEncoder.encode

    val ps = Seq(
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
