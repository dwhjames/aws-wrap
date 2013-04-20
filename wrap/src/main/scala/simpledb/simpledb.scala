
package aws.wrap
package simpledb

import scala.concurrent.{Future, ExecutionContext}
import scala.collection.JavaConverters._

import java.util.concurrent.ExecutorService

import com.amazonaws.services.simpledb._
import com.amazonaws.services.simpledb.model._

trait AmazonSimpleDBScalaClient {

  val client: AmazonSimpleDBAsyncClient

  def batchDeleteAttributes(
    batchDeleteAttributesRequest: BatchDeleteAttributesRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.batchDeleteAttributesAsync, batchDeleteAttributesRequest)

  def batchDeleteAttributes(
    domainName: String,
    items:      Seq[(String, Seq[(String, String)])]
  ): Future[Unit] =
    batchDeleteAttributes(
      new BatchDeleteAttributesRequest(
        domainName,
        items.map{ case (name, attributes) =>
          new DeletableItem(
            name,
            attributes.map{ case (name, value) =>
              new Attribute(name, value)
            }.asJava
          )
        }.asJava
      )
    )

  def batchPutAttributes(
    batchPutAttributesRequest: BatchPutAttributesRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.batchPutAttributesAsync, batchPutAttributesRequest)

  def batchPutAttributes(
    domainName: String,
    items:      Seq[(String, Seq[(String, String, Boolean)])]
  ): Future[Unit] =
    batchPutAttributes(
      new BatchPutAttributesRequest(
        domainName,
        items.map{ case (name, attributes) =>
          new ReplaceableItem(
            name,
            attributes.map{ case (name, value, replace) =>
              new ReplaceableAttribute(name, value, replace)
            }.asJava
          )
        }.asJava
      )
    )

  def createDomain(
    createDomainRequest: CreateDomainRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.createDomainAsync, createDomainRequest)

  def createDomain(
    domainName: String
  ): Future[Unit] =
    createDomain(new CreateDomainRequest(domainName))

  def deleteAttributes(
    deleteAttributesRequest: DeleteAttributesRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.deleteAttributesAsync, deleteAttributesRequest)

  def deleteAttributes(
    domainName: String,
    itemName:   String,
    attributes: Seq[(String, String)] = Seq.empty,
    expected:   UpdateCondition       = null
  ): Future[Unit] =
    deleteAttributes(
      new DeleteAttributesRequest(
        domainName,
        itemName,
        attributes.map(p => new Attribute(p._1, p._2)).asJava,
        expected
      )
    )

  def deleteDomain(
    deleteDomainRequest: DeleteDomainRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.deleteDomainAsync, deleteDomainRequest)

  def deleteDomain(
    domainName: String
  ): Future[Unit] =
    deleteDomain(new DeleteDomainRequest(domainName))

  def domainMetadata(
    domainMetadataRequest: DomainMetadataRequest
  ): Future[DomainMetadataResult] =
    wrapAsyncMethod(client.domainMetadataAsync, domainMetadataRequest)

  def domainMetadata(
    domainName: String
  ): Future[DomainMetadataResult] =
    domainMetadata(new DomainMetadataRequest(domainName))

  def getAttributes(
    getAttributesRequest: GetAttributesRequest
  ): Future[GetAttributesResult] =
    wrapAsyncMethod(client.getAttributesAsync, getAttributesRequest)

  def getAttributes(
    domainName:     String,
    itemName:       String,
    attributeNames: Iterable[String] = Iterable.empty,
    consistentRead: Boolean          = false
  ): Future[GetAttributesResult] =
    getAttributes(
      new GetAttributesRequest(domainName, itemName)
      .withAttributeNames(attributeNames.asJavaCollection)
      .withConsistentRead(consistentRead)
    )

  def getExecutorService(): ExecutorService =
    client.getExecutorService()

  def getExecutionContext(): ExecutionContext =
    ExecutionContext.fromExecutorService(client.getExecutorService())

  def listDomains(
    listDomainsRequest: ListDomainsRequest
  ): Future[ListDomainsResult] =
    wrapAsyncMethod(client.listDomainsAsync, listDomainsRequest)

  def listDomains(
    nextToken: String = null
  ): Future[ListDomainsResult] =
    listDomains(
      new ListDomainsRequest()
      .withNextToken(nextToken)
    )

  def putAttributes(
    putAttributesRequest: PutAttributesRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.putAttributesAsync, putAttributesRequest)

  def putAttributes(
    domainName: String,
    itemName:   String,
    attributes: Seq[(String, String, Boolean)],
    expected:   UpdateCondition = null
  ): Future[Unit] =
    putAttributes(
      new PutAttributesRequest(
        domainName,
        itemName,
        attributes.map{ case (n, v, r) =>
          new ReplaceableAttribute(n, v, r)
        }.asJava,
        expected
      )
    )

  def select(
    selectRequest: SelectRequest
  ): Future[SelectResult] =
    wrapAsyncMethod(client.selectAsync, selectRequest)

  def select(
    selectExpression: String,
    nextToken:        String  = null,
    consistentRead:   Boolean = false
  ): Future[SelectResult] =
    select(
      new SelectRequest(selectExpression)
      .withNextToken(nextToken)
      .withConsistentRead(consistentRead)
    )

  def shutdown(): Unit =
    client.shutdown()

}

object AmazonSimpleDBScalaClient {

  private class AmazonSimpleDBScalaClientImpl(override val client: AmazonSimpleDBAsyncClient) extends AmazonSimpleDBScalaClient

  def fromAsyncClient(client: AmazonSimpleDBAsyncClient): AmazonSimpleDBScalaClient =
    new AmazonSimpleDBScalaClientImpl(client)
}
