package aws.simpledb

import java.util.Date

import scala.annotation.implicitNotFound
import scala.util.{ Try, Success }
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import play.api.libs.ws._

import aws.core._
import aws.core.parsers._
import aws.simpledb.SDBParsers._

case class SDBAttribute(name: String, value: String, replace: Option[Boolean] = None)
case class SDBItem(name: String, attributes: Seq[SDBAttribute])
case class SDBDomain(name: String)
case class SDBDomainMetadata(
  timestamp: Date,
  itemCount: Long,
  attributeValueCount: Long,
  attributeNameCount: Long,
  itemNamesSizeBytes: Long,
  attributeValuesSizeBytes: Long,
  attributeNamesSizeBytes: Long
)

case class SDBRegion(name: String, host: String)
@implicitNotFound(
  "You need to import a region to specify which datacenter you want to use. If you don't care, just import aws.simpledb.SDBRegion.DEFAULT"
)
object SDBRegion {
  def apply(region: Region) = new SDBRegion(region.name, "sdb." + region.host)

  implicit val DEFAULT = SDBRegion(Region.DEFAULT)
  val US_EAST_1 = SDBRegion(Region.US_EAST_1)
  val US_WEST_1 = SDBRegion(Region.US_WEST_1)
  val US_WEST_2 = SDBRegion(Region.US_WEST_2)
  val EU_WEST_1 = SDBRegion(Region.EU_WEST_1)
  val ASIA_SOUTHEAST_1 = SDBRegion(Region.ASIA_SOUTHEAST_1)
  val ASIA_NORTHEAST_1 = SDBRegion(Region.ASIA_NORTHEAST_1)
  val SA_EAST_1 = SDBRegion(Region.SA_EAST_1)

}

object SimpleDB {

  object Parameters {
    def DomainName(a: String) = ("DomainName" -> a)
    def ItemName(a: String) = ("ItemName" -> a)
    def MaxNumberOfDomains(n: Int) = ("MaxNumberOfDomains" -> n.toString)
    def NextToken(n: String) = ("NextToken" -> n)
    def ConsistentRead(c: Boolean) = ("ConsistentRead" -> (if (c) "true" else "false"))
    def AttributeName(n: String) = ("AttributeName" -> n)
    def Attributes(attrs: Seq[SDBAttribute]): Seq[(String, String)] = (for ((attribute, i) <- attrs.zipWithIndex) yield {
      Seq(
        "Attribute.%d.Name".format(i + 1) -> attribute.name,
        "Attribute.%d.Value".format(i + 1) -> attribute.value
      ) ++ {
          if (attribute.replace.isDefined) Seq("Attribute.%s.Replace".format((i + 1).toString) -> attribute.replace.get.toString) else Nil
        }
    }).flatten
    def Items(items: Seq[SDBItem]): Seq[(String, String)] = (for ((item, i) <- items.zipWithIndex) yield {
      val prefix = "Item.%d.".format(i + 1)
      (prefix + "ItemName" -> item.name) +: Attributes(item.attributes).map { attr =>
        (prefix + attr._1 -> attr._2)
      }
    }).flatten
    def SelectExpression(expression: String) = ("SelectExpression" -> expression)
  }

  import AWS.Parameters._
  import Parameters._

  private def request(parameters: (String, String)*)(implicit region: SDBRegion): Future[Response] = {
    WS.url("https://" + region.host + "/?" + SimpleDBCalculator.url("GET", parameters)).get()
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
  def putAttributes(domainName: String,
                    itemName: String,
                    attributes: Seq[SDBAttribute])(implicit region: SDBRegion): Future[Try[EmptyResult]] = {
    val params = Seq(
      Action("PutAttributes"),
      DomainName(domainName),
      ItemName(itemName)
    ) ++ Attributes(attributes)

    request(params: _*).map { wsresponse =>
      aws.core.EmptyResult(wsresponse)
    }
  }

  /**
   * Delete all or some attributes for an item. To delete the whole item, pass its name with an empty attribute list.
   */
  def deleteAttributes(domainName: String, item: SDBItem)(implicit region: SDBRegion): Future[Try[EmptyResult]] = {
    val params = Seq(
      Action("DeleteAttributes"),
      DomainName(domainName),
      ItemName(item.name)
    ) ++ Attributes(item.attributes)

    request(params: _*).map { wsresponse =>
      aws.core.EmptyResult(wsresponse)
    }
  }

  /**
   * Gets one or all Attributes from the given domain name and item name
   */
  def getAttributes(domainName: String,
                    itemName: String,
                    attributeName: Option[String] = None,
                    consistentRead: Boolean = false)(implicit region: SDBRegion): Future[Try[SimpleResult[Seq[SDBAttribute]]]] = {
    val params = Seq(
      Action("GetAttributes"),
      DomainName(domainName),
      ItemName(itemName),
      ConsistentRead(consistentRead)
    ) ++ attributeName.map(AttributeName(_)).toSeq

    request(params: _*).map { wsresponse =>
      SimpleResult(wsresponse.xml, Parser.of[Seq[SDBAttribute]])
    }
  }

  /**
   * Get detailed information about a domain
   */
  def domainMetadata(domainName: String)(implicit region: SDBRegion): Future[Try[SimpleResult[SDBDomainMetadata]]] = {
    request(Action("DomainMetadata"), DomainName(domainName)).map { wsresponse =>
      SimpleResult(wsresponse.xml, Parser.of[SDBDomainMetadata])
    }
  }

  /**
   * Returns a set of Attributes for ItemNames that match the select expression (similar to SQL)
   */
  def select(expression: String, nextToken: Option[String] = None, consistentRead: Boolean = false)(implicit region: SDBRegion): Future[Try[SimpleResult[Seq[SDBItem]]]] = {
    val params = Seq(
      Action("Select"),
      SelectExpression(expression),
      ConsistentRead(consistentRead)
    ) ++ nextToken.map(NextToken(_)).toSeq

    request(params: _*).map { wsresponse =>
      SimpleResult(wsresponse.xml, Parser.of[Seq[SDBItem]])
    }
  }

  /**
   * Put attributes for more than one item
   */
  def batchPutAttributes(domainName: String, items: Seq[SDBItem])(implicit region: SDBRegion): Future[Try[Result]] = {
    val params = Seq(
      Action("BatchPutAttributes"),
      DomainName(domainName)
    ) ++ Items(items)
    request(params: _*).map { wsresponse =>
      aws.core.EmptyResult(wsresponse)
    }
  }

  /**
   * Delete attributes for more than one item
   */
  def batchDeleteAttributes(domainName: String, items: Seq[SDBItem])(implicit region: SDBRegion): Future[Try[Result]] = {
    val params = Seq(
      Action("BatchDeleteAttributes"),
      DomainName(domainName)
    ) ++ Items(items)
    request(params: _*).map { wsresponse =>
      aws.core.EmptyResult(wsresponse)
    }
  }

}

object SimpleDBCalculator {

  val VERSION = "2009-04-15"
  val SIGVERSION = "2"
  val SIGMETHOD = "HmacSHA1"

  def url(method: String, params: Seq[(String, String)])(implicit region: SDBRegion): String = {

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

    val toSign = "%s\n%s\n%s\n%s".format(method, region.host , "/", queryString)

    "Signature=" + encode(signature(toSign)) + "&" + queryString
  }

  def signature(data: String) = HmacSha1.calculate(data, AWS.secret)

}
