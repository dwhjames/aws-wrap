/*
 * Copyright 2012 Pellucid and Zenexity
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

package aws.dynamodb

sealed trait WriteRequest

case class PutRequest(item: Map[String, DDBAttribute]) extends WriteRequest

case class DeleteRequest(key: KeyValue) extends WriteRequest

/**
 * Used for the response to [[DynamoDB.batchWriteItem]]
 * @param successful requests: a Map of [[QueryResponses]] per table
 * @param unsuccessful requests, per table
 */
case class BatchWriteResponse(responses: Seq[(String, QueryResponse)], unprocessed: Seq[(String, Seq[WriteRequest])])

case class GetRequest(keys: Seq[KeyValue], attributesToGet: Seq[String] = Nil, consistentRead: Boolean = false)

/**
 * Used for the response to [[DynamoDB.batchGetItem]]
 * @param successful requests: a Map of [[QueryResponses]] per table
 * @param unsuccessful requests, per table
 */
case class BatchGetResponse(responses: Seq[(String, QueryResponse)], unprocessed: Seq[(String, Seq[GetRequest])])

