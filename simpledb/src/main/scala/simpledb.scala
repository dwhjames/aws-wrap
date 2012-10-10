package aws.simpledb

import java.util.Date

import scala.annotation.implicitNotFound
import scala.util.{ Try, Success }
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import play.api.libs.ws._

import aws.core._
import aws.core.Types._
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

case class SimpleDBMeta(requestId: String, boxUsage: String) extends Metadata

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

  private def tryParse[M <: Metadata, T](resp: Response)(implicit p: Parser[Result[M, T]]) = Parser.parse[Result[M, T]](resp).fold(
    e => throw new RuntimeException(e),
    identity)

  /**
   * This helper will fetch data using request, and try to parse the response to the specified types
   */
  private def get[M <: Metadata, T](parameters: (String, String)*)(implicit region: AWSRegion, p: Parser[Result[M, T]]): Future[Result[M, T]] =
    request(parameters: _*).map(tryParse[M, T])

  /**
   * Creates a domain with the given name
   */
  def createDomain(domainName: String)(implicit region: AWSRegion): Future[EmptyResult[SimpleDBMeta]] =
    get[SimpleDBMeta, Unit](Action("CreateDomain"), DomainName(domainName))

  /**
   * Deletes the given domain
   */
  def deleteDomain(domainName: String)(implicit region: AWSRegion): Future[EmptyResult[SimpleDBMeta]] =
    get[SimpleDBMeta, Unit](Action("DeleteDomain"), DomainName(domainName))

  /**
   * Lists domains starting with the nextToken, if present, and giving the max number of domains given
   */
  def listDomains(maxNumberOfDomains: Int = 100, nextToken: Option[String] = None)(implicit region: AWSRegion): Future[Result[SimpleDBMeta, Seq[SDBDomain]]] = {
    val params = Seq(
      Action("ListDomains"),
      MaxNumberOfDomains(maxNumberOfDomains)) ++ nextToken.map(NextToken(_)).toSeq

    get[SimpleDBMeta, Seq[SDBDomain]](params: _*)
  }

  /**
   * Puts the attributes into SimpleDB
   */
  def putAttributes(domainName: String,
                    itemName: String,
                    attributes: Seq[SDBAttribute])(implicit region: AWSRegion): Future[EmptyResult[SimpleDBMeta]] = {
    val params = Seq(
      Action("PutAttributes"),
      DomainName(domainName),
      ItemName(itemName)) ++ Attributes(attributes)

    get[SimpleDBMeta, Unit](params: _*)
  }

  /**
   * Delete all or some attributes for an item. To delete the whole item, pass its name with an empty attribute list.
   */
  def deleteAttributes(domainName: String, item: SDBItem)(implicit region: AWSRegion): Future[EmptyResult[SimpleDBMeta]] = {
    val params = Seq(
      Action("DeleteAttributes"),
      DomainName(domainName),
      ItemName(item.name)) ++ Attributes(item.attributes)

    get[SimpleDBMeta, Unit](params: _*)
  }

  /**
   * Gets one or all Attributes from the given domain name and item name
   */
  def getAttributes(domainName: String,
                    itemName: String,
                    attributeName: Option[String] = None,
                    consistentRead: Boolean = false)(implicit region: AWSRegion): Future[Result[SimpleDBMeta, Seq[SDBAttribute]]] = {
    val params = Seq(
      Action("GetAttributes"),
      DomainName(domainName),
      ItemName(itemName),
      ConsistentRead(consistentRead)) ++ attributeName.map(AttributeName(_)).toSeq

    get[SimpleDBMeta, Seq[SDBAttribute]](params: _*)
  }

  /**
   * Get detailed information about a domain
   */
  def domainMetadata(domainName: String)(implicit region: AWSRegion): Future[Result[SimpleDBMeta, SDBDomainMetadata]] =
    get[SimpleDBMeta, SDBDomainMetadata](Action("DomainMetadata"), DomainName(domainName))

  /**
   * Returns a set of Attributes for ItemNames that match the select expression (similar to SQL)
   */
  def select(expression: String, nextToken: Option[String] = None, consistentRead: Boolean = false)(implicit region: AWSRegion): Future[Result[SimpleDBMeta, Seq[SDBItem]]] = {
    val params = Seq(
      Action("Select"),
      SelectExpression(expression),
      ConsistentRead(consistentRead)) ++ nextToken.map(NextToken(_)).toSeq

    get[SimpleDBMeta, Seq[SDBItem]](params: _*)
  }

  /**
   * Put attributes for more than one item
   */
  def batchPutAttributes(domainName: String, items: Seq[SDBItem])(implicit region: AWSRegion): Future[EmptyResult[SimpleDBMeta]] = {
    val params = Seq(
      Action("BatchPutAttributes"),
      DomainName(domainName)) ++ Items(items)
    get[SimpleDBMeta, Unit](params: _*)
  }

  /**
   * Delete attributes for more than one item
   */
  def batchDeleteAttributes(domainName: String, items: Seq[SDBItem])(implicit region: AWSRegion): Future[EmptyResult[SimpleDBMeta]] = {
    val params = Seq(
      Action("BatchDeleteAttributes"),
      DomainName(domainName)) ++ Items(items)
    get[SimpleDBMeta, Unit](params: _*)
  }

}

