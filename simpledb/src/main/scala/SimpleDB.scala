package aws.simpledb

import java.util.Date

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import play.api.libs.ws._

import aws.core._
import aws.core.Types._
import aws.core.parsers._
import aws.core.signature.V2

import aws.simpledb.SDBParsers._

case class SimpleDBMeta(requestId: String, boxUsage: String) extends Metadata

object SimpleDB extends V2[SimpleDBMeta] {

  private object Parameters {
    def DomainName(a: String) = ("DomainName" -> a)
    def ItemName(a: String) = ("ItemName" -> a)
    def MaxNumberOfDomains(n: Int) = ("MaxNumberOfDomains" -> n.toString)
    def NextToken(n: String) = ("NextToken" -> n)
    def ConsistentRead(c: Boolean) = ("ConsistentRead" -> (if (c) "true" else "false"))
    def AttributeName(n: String) = ("AttributeName" -> n)
    def Attributes(attrs: Seq[SDBAttribute]): Seq[(String, String)] = (for ((attribute, i) <- attrs.zipWithIndex) yield {
      Seq(
        "Attribute.%d.Name".format(i + 1) -> attribute.name,
        "Attribute.%d.Value".format(i + 1) -> attribute.value,
        "Attribute.%s.Replace".format(i + 1) -> (if (attribute.replace) "true" else "false"))
    }).flatten
    def Items(items: Seq[SDBItem]): Seq[(String, String)] = (for ((item, i) <- items.zipWithIndex) yield {
      val prefix = "Item.%d.".format(i + 1)
      (prefix + "ItemName" -> item.name) +: Attributes(item.attributes).map { attr =>
        (prefix + attr._1 -> attr._2)
      }
    }).flatten
    def Expected(expected: Seq[SDBExpected]): Seq[(String, String)] = (for ((attribute, i) <- expected.zipWithIndex) yield {
      Seq("Attribute.%d.Name".format(i + 1) -> attribute.name) ++ attribute.value.map { v =>
        Seq("Attribute.%d.Value".format(i + 1) -> v)
      }.getOrElse(
        Seq("Attribute.%d.Expected".format(i + 1) -> "true"))
    }).flatten
    def SelectExpression(expression: String) = ("SelectExpression" -> expression)
  }

  import AWS.Parameters._
  import Parameters._

  /**
   * Creates a new domain. The domain name must be unique among the domains associated with the Access Key ID used.
   * The CreateDomain operation might take 10 or more seconds to complete.
   * CreateDomain is an idempotent operation; running it multiple times using the same domain name will not result
   * in an error response.
   *
   * You can create up to 250 domains per account.
   * If you require additional domains, go to http://aws.amazon.com/contact-us/simpledb-limit-request/.
   *
   * @param domainName
   */
  def createDomain(domainName: String)(implicit region: SDBRegion): Future[EmptyResult[SimpleDBMeta]] =
    get[Unit](Action("CreateDomain"), DomainName(domainName))

  /**
   * Deletes the given domain. Any items (and their attributes) in the domain are deleted as well.
   * The DeleteDomain operation might take 10 or more seconds to complete.
   *
   * Running DeleteDomain on a domain that does not exist or running the function multiple times
   * using the same domain name will not result in an error response.
   *
   * @param domainName
   */
  def deleteDomain(domainName: String)(implicit region: SDBRegion): Future[EmptyResult[SimpleDBMeta]] =
    get[Unit](Action("DeleteDomain"), DomainName(domainName))

  /**
   * Lists all domains associated with the Access Key ID.
   * It returns domain names up to the limit set by MaxNumberOfDomains. A NextToken is returned if
   * there are more than MaxNumberOfDomains domains. Calling ListDomains successive times with the NextToken returns up to MaxNumberOfDomains more domain names each time.
   *
   * @param maxNumberOfDomains limit the response to a size (default is 100)
   * @param nextToken: optionally provide this value you got from a previous request to get the next page
   */
  def listDomains(maxNumberOfDomains: Int = 100, nextToken: Option[String] = None)(implicit region: SDBRegion): Future[Result[SimpleDBMeta, Seq[SDBDomain]]] = {
    val params = Seq(
      Action("ListDomains"),
      MaxNumberOfDomains(maxNumberOfDomains)) ++ nextToken.map(NextToken(_)).toSeq

    get[Seq[SDBDomain]](params: _*)
  }

  /**
   * Creates or replaces attributes in an item.
   *
   * Using PutAttributes to replace attribute values that do not exist will not result in an error response.
   *
   * When using eventually consistent reads, a GetAttributes or Select request (read) immediately after a DeleteAttributes or PutAttributes request (write) might not return the updated data. A consistent read always reflects all writes that received a successful response prior to the read. For more information, see Consistency.
   *
   * @param domainName
   * @param itemName the item to update. May not be empty.
   * @param attributes the attributes to create or update
   * @param expected if defined, perform the expected conditional check on one attribute. If expected.value is None,
   *                 will check that the attribute exists. If expected.value is defined, will check that the attribute
   *                 exists and has the specified value.
   */
  def putAttributes(domainName: String,
                    itemName: String,
                    attributes: Seq[SDBAttribute],
                    expected: Seq[SDBExpected] = Nil)(implicit region: SDBRegion): Future[EmptyResult[SimpleDBMeta]] = {
    val params = Seq(
      Action("PutAttributes"),
      DomainName(domainName),
      ItemName(itemName)) ++ Attributes(attributes) ++ Expected(expected)

    get[Unit](params: _*)
  }

  /**
   * Deletes one or more attributes associated with the item. If all attributes of an item are deleted, the item is deleted.
   *
   * If you specify DeleteAttributes without attributes or values, all the attributes for the item are deleted.
   *
   * Unless you specify conditions, the DeleteAttributes is an idempotent operation; running it multiple times on the same item or attribute
   * does not result in an error response.
   *
   * Conditional deletes are useful for only deleting items and attributes if specific conditions are met. If the conditions are met, Amazon SimpleDB performs the delete. Otherwise, the data is not deleted.
   *
   * When using eventually consistent reads, a GetAttributes or Select request (read) immediately after a DeleteAttributes or PutAttributes request (write) might not return the updated data. A consistent read always reflects all writes that received a successful response prior to the read.
   * For more information, see [[http://docs.amazonwebservices.com/AmazonSimpleDB/latest/DeveloperGuide/ConsistencySummary.html Consistency]].
   *
   * @param domainName
   * @param itemName the item to update. May not be empty.
   * @param attributes the list of attributes to delete
   * @param expected if defined, perform the expected conditional check on one attribute. If expected.value is None,
   *                 will check that the attribute exists. If expected.value is defined, will check that the attribute
   *                 exists and has the specified value.
   *
   */
  def deleteAttributes(domainName: String,
                       itemName: String,
                       attributes: Seq[SDBAttribute] = Nil,
                       expected: Seq[SDBExpected] = Nil)(implicit region: SDBRegion): Future[EmptyResult[SimpleDBMeta]] = {
    val params = Seq(
      Action("DeleteAttributes"),
      DomainName(domainName),
      ItemName(itemName)) ++ Attributes(attributes) ++ Expected(expected)

    get[Unit](params: _*)
  }

  /**
   * Returns all of the attributes associated with the item. Optionally, the attributes returned can be limited to one specified attribute name parameters.
   * Amazon SimpleDB keeps multiple copies of each domain. When data is written or updated, all copies of the data are updated. However, it takes time for
   * the update to propagate to all storage locations. The data will eventually be consistent, but an immediate read might not show the change. If eventually
   * consistent reads are not acceptable for your application, use ConsistentRead. Although this operation might take longer than a standard read, it always
   * returns the last updated value.
   *
   * @param domainName
   * @param itemName
   * @param attributeName
   * @param consistentRead if true, Amazon will always return the last updated value. If false, the response will be faster but the data may not be the most recent.
   */
  def getAttributes(domainName: String,
                    itemName: String,
                    attributeName: Option[String] = None,
                    consistentRead: Boolean = false)(implicit region: SDBRegion): Future[Result[SimpleDBMeta, Seq[SDBAttribute]]] = {
    val params = Seq(
      Action("GetAttributes"),
      DomainName(domainName),
      ItemName(itemName),
      ConsistentRead(consistentRead)) ++ attributeName.map(AttributeName(_)).toSeq

    get[Seq[SDBAttribute]](params: _*)
  }

  /**
   * Get detailed information about a domain
   */
  def domainMetadata(domainName: String)(implicit region: SDBRegion): Future[Result[SimpleDBMeta, SDBDomainMetadata]] =
    get[SDBDomainMetadata](Action("DomainMetadata"), DomainName(domainName))

  /**
   * Returns a set of Attributes for ItemNames that match the select expression (similar to SQL)
   */
  def select(expression: String, nextToken: Option[String] = None, consistentRead: Boolean = false)(implicit region: SDBRegion): Future[Result[SimpleDBMeta, Seq[SDBItem]]] = {
    val params = Seq(
      Action("Select"),
      SelectExpression(expression),
      ConsistentRead(consistentRead)) ++ nextToken.map(NextToken(_)).toSeq

    get[Seq[SDBItem]](params: _*)
  }

  /**
   * Put attributes for more than one item
   */
  def batchPutAttributes(domainName: String, items: Seq[SDBItem])(implicit region: SDBRegion): Future[EmptyResult[SimpleDBMeta]] = {
    val params = Seq(
      Action("BatchPutAttributes"),
      DomainName(domainName)) ++ Items(items)
    get[Unit](params: _*)
  }

  /**
   * Delete attributes for more than one item
   */
  def batchDeleteAttributes(domainName: String, items: Seq[SDBItem])(implicit region: SDBRegion): Future[EmptyResult[SimpleDBMeta]] = {
    val params = Seq(
      Action("BatchDeleteAttributes"),
      DomainName(domainName)) ++ Items(items)
    get[Unit](params: _*)
  }

}

