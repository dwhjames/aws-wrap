package aws.simpledb

import java.util.Date

import scala.annotation.implicitNotFound
import scala.util.{ Try, Success }
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import play.api.libs.ws._

import aws.core._
import aws.core.parsers._
import aws.core.signature.V2
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
  attributeNamesSizeBytes: Long)

object SDBRegion {

  val NAME = "sdb"

  val US_EAST_1 = AWSRegion.US_EAST_1(NAME)
  val US_WEST_1 = AWSRegion.US_WEST_1(NAME)
  val US_WEST_2 = AWSRegion.US_WEST_2(NAME)
  val EU_WEST_1 = AWSRegion.EU_WEST_1(NAME)
  val ASIA_SOUTHEAST_1 = AWSRegion.ASIA_SOUTHEAST_1(NAME)
  val ASIA_NORTHEAST_1 = AWSRegion.ASIA_NORTHEAST_1(NAME)
  val SA_EAST_1 = AWSRegion.SA_EAST_1(NAME)

  implicit val DEFAULT = US_EAST_1
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
        "Attribute.%d.Value".format(i + 1) -> attribute.value) ++ {
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

  private def request(parameters: (String, String)*)(implicit region: AWSRegion): Future[Response] = {
    WS.url("https://" + region.host + "/?" + V2.signedUrl("GET", parameters)).get()
  }

  /**
   * Creates a domain with the given name
   */
  def createDomain(domainName: String)(implicit region: AWSRegion): Future[Try[Result]] = {
    request(Action("CreateDomain"), DomainName(domainName)).map { wsresponse =>
      EmptyResult(wsresponse)
    }
  }

  /**
   * Deletes the given domain
   */
  def deleteDomain(domainName: String)(implicit region: AWSRegion): Future[Try[Result]] = {
    request(Action("DeleteDomain"), DomainName(domainName)).map { wsresponse =>
      EmptyResult(wsresponse)
    }
  }

  /**
   * Lists domains starting with the nextToken, if present, and giving the max number of domains given
   */
  def listDomains(maxNumberOfDomains: Int = 100, nextToken: Option[String] = None)(implicit region: AWSRegion): Future[Try[SimpleResult[Seq[SDBDomain]]]] = {
    val params = Seq(
      Action("ListDomains"),
      MaxNumberOfDomains(maxNumberOfDomains)) ++ nextToken.map(NextToken(_)).toSeq

    request(params: _*).map { wsresponse =>
      SimpleResult[Seq[SDBDomain]](wsresponse)
    }
  }

  /**
   * Puts the attributes into SimpleDB
   */
  def putAttributes(domainName: String,
                    itemName: String,
                    attributes: Seq[SDBAttribute])(implicit region: AWSRegion): Future[Try[EmptyResult]] = {
    val params = Seq(
      Action("PutAttributes"),
      DomainName(domainName),
      ItemName(itemName)) ++ Attributes(attributes)

    request(params: _*).map { wsresponse =>
      EmptyResult(wsresponse)
    }
  }

  /**
   * Delete all or some attributes for an item. To delete the whole item, pass its name with an empty attribute list.
   */
  def deleteAttributes(domainName: String, item: SDBItem)(implicit region: AWSRegion): Future[Try[EmptyResult]] = {
    val params = Seq(
      Action("DeleteAttributes"),
      DomainName(domainName),
      ItemName(item.name)) ++ Attributes(item.attributes)

    request(params: _*).map { wsresponse =>
      EmptyResult(wsresponse)
    }
  }

  /**
   * Gets one or all Attributes from the given domain name and item name
   */
  def getAttributes(domainName: String,
                    itemName: String,
                    attributeName: Option[String] = None,
                    consistentRead: Boolean = false)(implicit region: AWSRegion): Future[Try[SimpleResult[Seq[SDBAttribute]]]] = {
    val params = Seq(
      Action("GetAttributes"),
      DomainName(domainName),
      ItemName(itemName),
      ConsistentRead(consistentRead)) ++ attributeName.map(AttributeName(_)).toSeq

    request(params: _*).map { wsresponse =>
      SimpleResult[Seq[SDBAttribute]](wsresponse)
    }
  }

  /**
   * Get detailed information about a domain
   */
  def domainMetadata(domainName: String)(implicit region: AWSRegion): Future[Try[SimpleResult[SDBDomainMetadata]]] = {
    request(Action("DomainMetadata"), DomainName(domainName)).map { wsresponse =>
      SimpleResult[SDBDomainMetadata](wsresponse)
    }
  }

  /**
   * Returns a set of Attributes for ItemNames that match the select expression (similar to SQL)
   */
  def select(expression: String, nextToken: Option[String] = None, consistentRead: Boolean = false)(implicit region: AWSRegion): Future[Try[SimpleResult[Seq[SDBItem]]]] = {
    val params = Seq(
      Action("Select"),
      SelectExpression(expression),
      ConsistentRead(consistentRead)) ++ nextToken.map(NextToken(_)).toSeq

    request(params: _*).map { wsresponse =>
      SimpleResult[Seq[SDBItem]](wsresponse)
    }
  }

  /**
   * Put attributes for more than one item
   */
  def batchPutAttributes(domainName: String, items: Seq[SDBItem])(implicit region: AWSRegion): Future[Try[Result]] = {
    val params = Seq(
      Action("BatchPutAttributes"),
      DomainName(domainName)) ++ Items(items)
    request(params: _*).map { wsresponse =>
      EmptyResult(wsresponse)
    }
  }

  /**
   * Delete attributes for more than one item
   */
  def batchDeleteAttributes(domainName: String, items: Seq[SDBItem])(implicit region: AWSRegion): Future[Try[Result]] = {
    val params = Seq(
      Action("BatchDeleteAttributes"),
      DomainName(domainName)) ++ Items(items)
    request(params: _*).map { wsresponse =>
      EmptyResult(wsresponse)
    }
  }

}

