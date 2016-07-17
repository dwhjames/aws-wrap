/*
 * Copyright 2012-2015 Pellucid Analytics
 * Copyright 2015 Daniel W. H. James
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.dwhjames.awswrap
package simpledb

import scala.concurrent.{Future, ExecutionContext}
import scala.collection.JavaConverters._

import java.util.concurrent.ExecutorService

import com.amazonaws.services.simpledb._
import com.amazonaws.services.simpledb.model._

class AmazonSimpleDBScalaClient(val client: AmazonSimpleDBAsyncClient) {

  def batchDeleteAttributes(
    batchDeleteAttributesRequest: BatchDeleteAttributesRequest
  ): Future[BatchDeleteAttributesResult] =
    wrapAsyncMethod[BatchDeleteAttributesRequest, BatchDeleteAttributesResult](client.batchDeleteAttributesAsync, batchDeleteAttributesRequest)

  def batchDeleteAttributes(
    domainName: String,
    items:      Seq[(String, Seq[(String, String)])]
  ): Future[BatchDeleteAttributesResult] =
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
  ): Future[BatchPutAttributesResult] =
    wrapAsyncMethod[BatchPutAttributesRequest, BatchPutAttributesResult](client.batchPutAttributesAsync, batchPutAttributesRequest)

  def batchPutAttributes(
    domainName: String,
    items:      Seq[(String, Seq[(String, String, Boolean)])]
  ): Future[BatchPutAttributesResult] =
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
  ): Future[CreateDomainResult] =
    wrapAsyncMethod[CreateDomainRequest, CreateDomainResult](client.createDomainAsync, createDomainRequest)

  def createDomain(
    domainName: String
  ): Future[CreateDomainResult] =
    createDomain(new CreateDomainRequest(domainName))

  def deleteAttributes(
    deleteAttributesRequest: DeleteAttributesRequest
  ): Future[DeleteAttributesResult] =
    wrapAsyncMethod[DeleteAttributesRequest,DeleteAttributesResult](client.deleteAttributesAsync, deleteAttributesRequest)

  def deleteAttributes(
    domainName: String,
    itemName:   String,
    attributes: Seq[(String, String)] = Seq.empty,
    expected:   UpdateCondition       = null
  ): Future[DeleteAttributesResult] =
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
  ): Future[DeleteDomainResult] =
    wrapAsyncMethod[DeleteDomainRequest, DeleteDomainResult](client.deleteDomainAsync, deleteDomainRequest)

  def deleteDomain(
    domainName: String
  ): Future[DeleteDomainResult] =
    deleteDomain(new DeleteDomainRequest(domainName))

  def domainMetadata(
    domainMetadataRequest: DomainMetadataRequest
  ): Future[DomainMetadataResult] =
    wrapAsyncMethod[DomainMetadataRequest,DomainMetadataResult](client.domainMetadataAsync, domainMetadataRequest)

  def domainMetadata(
    domainName: String
  ): Future[DomainMetadataResult] =
    domainMetadata(new DomainMetadataRequest(domainName))

  def getAttributes(
    getAttributesRequest: GetAttributesRequest
  ): Future[GetAttributesResult] =
    wrapAsyncMethod[GetAttributesRequest, GetAttributesResult](client.getAttributesAsync, getAttributesRequest)

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
    wrapAsyncMethod[ListDomainsRequest, ListDomainsResult](client.listDomainsAsync, listDomainsRequest)

  def listDomains(
    nextToken: String = null
  ): Future[ListDomainsResult] =
    listDomains(
      new ListDomainsRequest()
      .withNextToken(nextToken)
    )

  def putAttributes(
    putAttributesRequest: PutAttributesRequest
  ): Future[PutAttributesResult] =
    wrapAsyncMethod[PutAttributesRequest, PutAttributesResult](client.putAttributesAsync, putAttributesRequest)

  def putAttributes(
    domainName: String,
    itemName:   String,
    attributes: Seq[(String, String, Boolean)],
    expected:   UpdateCondition = null
  ): Future[PutAttributesResult] =
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
    wrapAsyncMethod[SelectRequest, SelectResult](client.selectAsync, selectRequest)

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
