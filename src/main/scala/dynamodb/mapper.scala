/*
 * Copyright 2013 Pellucid Analytics
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

package com.pellucid.wrap
package dynamodb

import scala.concurrent.{Future, ExecutionContext}
import scala.collection.JavaConverters._
import scala.collection.mutable

import java.util.{Map => JMap}

import com.amazonaws.services.dynamodbv2.model._

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
  * A trait for serializers that convert Scala objects
  * to and from DynamoDB items.
  *
  * @tparam T the object type of this serializer
  */
trait DynamoDBSerializer[T] {

  /**
    * The DynamoDB table that this serializer operates on.
    */
  def tableName: String

  /**
    * The name of the attribute that forms the
    * primary hash key.
    */
  def hashAttributeName: String

  /**
    * The name of the attribute that forms the
    * primary range key.
    *
    * This is optional, as a table may not have
    * a range key.
    */
  def rangeAttributeName: Option[String] = None

  /**
    * Converts a DynamoDB item into a Scala object.
    *
    * @param item
    *     A map from attribute names to attribute values.
    * @return the deserialized object of type T.
    */
  def fromAttributeMap(item: mutable.Map[String, AttributeValue]): T

  /**
    * Converts a Scala object into a DynamoDB item.
    *
    * @param obj
    *     An object of type T.
    * @return a map from attribute names to attribute values.
    */
  def toAttributeMap(obj: T): Map[String, AttributeValue]

  /*
   * A helper for implementing toAttributeMap.
   *
   * {{{
   * override def toAttributeMap(obj: Foo): Map[String, AttributeValue] =
   *   Map(
   *     mkAtrribute("company", obj.company),
   *     ...
   *   )
   * }}}
   */
  protected def mkAttribute[K](name: String, value: K)(implicit conv: K => AttributeValue): (String, AttributeValue) =
    (name, conv(value))

  /*
   * A helper for implementing toAttributeMap.
   *
   * {{{
   * override def toAttributeMap(obj: Foo): Map[String, AttributeValue] =
   *   Map(
   *     mkAtrribute("company" -> obj.company),
   *     ...
   *   )
   * }}}
   */
  protected def mkAttribute[K](pair: (String, K))(implicit conv: K => AttributeValue): (String, AttributeValue) =
    (pair._1, conv(pair._2))

  /**
    * Converts a Scala object into a DynamoDB key.
    *
    * The key is represented as a map. The concrete implementation
    * of a serializer may want to override this method for
    * efficiency reasons, as the default implementation uses
    * [[toAttributeMap]].
    *
    * @param obj
    *     An object of type T.
    * @return a map from attribute names to attribute values.
    */
  def primaryKeyOf(obj: T): Map[String, AttributeValue] = {
    val attributes = toAttributeMap(obj)
    val builder = Map.newBuilder[String, AttributeValue]
    builder += (hashAttributeName -> attributes(hashAttributeName))
    if (rangeAttributeName.isDefined)
      builder += (rangeAttributeName.get -> attributes(rangeAttributeName.get))
    builder.result
  }

  /**
    * Converts a hash key value into a DynamoDB key.
    *
    * The key is represented as a map.
    *
    * @param hashKey
    *     An value that is convertable to an [[com.pellucid.wrap.dynamodb.AttributeValue AttributeValue]].
    * @return a map from attribute names to attribute values.
    */
  def makeKey[K](hashKey: K)(implicit conv: K => AttributeValue): Map[String, AttributeValue] =
    Map(hashAttributeName -> conv(hashKey))

  /**
    * Converts hash and range key values into a DynamoDB key.
    *
    * The key is represented as a map.
    *
    * @param hashKey
    *     An value that is convertable to an [[com.pellucid.wrap.dynamodb.AttributeValue AttributeValue]].
    * @param rangeKey
    *     An value that is convertable to an [[com.pellucid.wrap.dynamodb.AttributeValue AttributeValue]].
    * @return a map from attribute names to attribute values.
    */
  def makeKey[K1, K2](
    hashKey: K1,
    rangeKey: K2
  )(implicit
    conv1: K1 => AttributeValue,
    conv2: K2 => AttributeValue
  ): Map[String, AttributeValue] =
    Map(
      hashAttributeName -> conv1(hashKey),
      (rangeAttributeName getOrElse {
         throw new UnsupportedOperationException(s"DynamoDBSerializer.makeKey: table $tableName does not have a range key")
       } ) -> conv2(rangeKey)
    )
}

/**
  * A trait for configuring [[AmazonDynamoDBScalaMapper]].
  */
trait AmazonDynamoDBScalaMapperConfig {

  /**
    * Transform a table name.
    *
    * Concrete implementations will rewrite
    * tables names, given an input table name.
    *
    * @param tableName
    *     the table name to transform.
    * @return the transformed table name.
    */
  def transformTableName(tableName: String): String

  /**
    * Choose the read consistency behavior.
    *
    * `true` configures the mapper for consistent reads.
    */
  val consistentReads: Boolean
}

/**
  * A factory for [[AmazonDynamoDBScalaMapperConfig]].
  */
object AmazonDynamoDBScalaMapperConfig {

  /**
    * Construct a mapper configuration.
    *
    * @param nameFunction
    *     the transformation to apply to the table name.
    * @param consistent
    *     set the consistency of reads.
    * @return a new configuration
    */
  def apply(
    nameFunction: String => String = identity,
    consistent:   Boolean          = false
  ): AmazonDynamoDBScalaMapperConfig = new AmazonDynamoDBScalaMapperConfig {
    override def transformTableName(tableName: String) =
      nameFunction(tableName)
    override val consistentReads = consistent
  }

  /**
    * A default [[AmazonDynamoDBScalaMapperConfig]].
    *
    * Provides a default configuration for
    * [[AmazonDynamoDBScalaMapper]].
    */
  object Default extends AmazonDynamoDBScalaMapperConfig {

    /**
      * Returns a table name untransformed.
      *
      * The default transformation on table names is
      * the identity transformation.
      *
      * @param table
      *     the table name to transform.
      * @return the same table name.
      */
    override def transformTableName(tableName: String) = tableName

    /**
      * The default is eventual consistency.
      */
    override val consistentReads = false
  }
}

/**
  * An object mapping for DynamoDB items.
  *
  * This trait provides the interface to an object mapper for DynamoDB.
  * It depends on a concrete implementation of [[AmazonDynamoDBScalaClient]].
  */
trait AmazonDynamoDBScalaMapper {

  private type DynamoDBKey = JMap[String, AttributeValue]

  /**
    * An abstract [[AmazonDynamoDBScalaClient]].
    */
  val client: AmazonDynamoDBScalaClient

  /**
    * An abstract ExecutionContext.
    */
  protected implicit val execCtx: ExecutionContext

  /**
    * The mapping configuration.
    *
    * [[AmazonDynamoDBScalaMapperConfig.Default]] is used by default.
    */
  protected val config: AmazonDynamoDBScalaMapperConfig =
    AmazonDynamoDBScalaMapperConfig.Default

  /**
    * Returns the table name.
    *
    * Determines the table name, by transforming the table name
    * of the implict serializer using the mapper's configuration.
    *
    * @param serializer
    *     the object serializer.
    * @return the transformed table name.
    */
  protected def tableName[T](implicit serializer: DynamoDBSerializer[T]): String =
    config.transformTableName(serializer.tableName)

  private val logger: Logger = LoggerFactory.getLogger(classOf[AmazonDynamoDBScalaMapper])



  /**
    * A method overloading container for [[deleteByKey]].
    *
    * This class contains the overloaded implementations
    * of [[deleteByKey]].
    *
    * @tparam T
    *     the type of the object to be deleted.
    * @see [[deleteByKey]]
    */
  class DeleteByKey[T] {

    /**
      * Delete a DynamoDB item by a hash key.
      *
      * If an item with the given key was deleted then the
      * deleted object returned, otherwise none.
      *
      * @tparam K
      *     a type that is viewable as an [[AttributeValue]].
      * @param hashKey
      *     a string, number, or byte array that is the hash key value of the
      *     item to be deleted.
      * @param serializer
      *     an implicit object serializer.
      * @return the deleted object, or None, in a future.
      * @see [[deleteByKey]]
      */
    def apply[K <% AttributeValue]
             (hashKey: K)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Option[T]] = {
      val request =
        new DeleteItemRequest()
        .withTableName(tableName)
        .withKey(serializer.makeKey(hashKey).asJava)
        .withReturnValues(ReturnValue.ALL_OLD)
      if (logger.isDebugEnabled)
        request.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

      client.deleteItem(request) map { result =>
        if (logger.isDebugEnabled)
          logger.debug(s"deleteByKey() ConsumedCapacity = ${result.getConsumedCapacity()}")

        Option { result.getAttributes } map { item =>
          serializer.fromAttributeMap(item.asScala)
        }
      }
    }

    /**
      * Delete a DynamoDB item by a hash key and range key.
      *
      * If an item with the given key was deleted then the
      * deleted object returned, otherwise none.
      *
      * @tparam K1
      *     a type that is viewable as an [[AttributeValue]].
      * @tparam K2
      *     a type that is viewable as an [[AttributeValue]].
      * @param hashKey
      *     a string, number, or byte array that is the hash key value of the
      *     item to be deleted.
      * @param rangeKey
      *     a string, number, or byte array that is the range key value of the
      *     item to be deleted.
      * @param serializer
      *     an implicit object serializer.
      * @return the deleted object, or None, in a future.
      * @see [[deleteByKey]]
      */
    def apply[K1 <% AttributeValue, K2 <% AttributeValue]
             (hashKey: K1, rangeKey: K2)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Option[T]] = {
      val request =
        new DeleteItemRequest()
        .withTableName(tableName)
        .withKey(serializer.makeKey(hashKey, rangeKey).asJava)
        .withReturnValues(ReturnValue.ALL_OLD)
      if (logger.isDebugEnabled)
        request.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

      client.deleteItem(request) map { result =>
        if (logger.isDebugEnabled)
          logger.debug(s"deleteByKey() ConsumedCapacity = ${result.getConsumedCapacity()}")

        Option { result.getAttributes } map { item =>
          serializer.fromAttributeMap(item.asScala)
        }
      }
    }
  }

  /**
    * Delete a DynamoDB item by a hash key (and range key).
    *
    * If an item with the given key was deleted then the
    * deleted object returned, otherwise none.
    *
    * @tparam T
    *     the type of the object to be deleted.
    * @see [[DeleteByKey]]
    */
  def deleteByKey[T] = new DeleteByKey[T]



  /**
    * Delete the DynamoDB item that corresponds to the given object
    *
    * @tparam T
    *     the type of the object to delete.
    * @param obj
    *     the object to delete.
    * @param serializer
    *     an implicit object serializer.
    */
  def delete[T](
    obj: T
  )(implicit serializer: DynamoDBSerializer[T]): Future[Unit] = {
    val request =
      new DeleteItemRequest()
      .withTableName(tableName)
      .withKey(serializer.primaryKeyOf(obj).asJava)
    if (logger.isDebugEnabled)
      request.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

    client.deleteItem(request) map { result =>
      if (logger.isDebugEnabled)
        logger.debug(s"delete() ConsumedCapacity = ${result.getConsumedCapacity()}")
    }
  }



  /**
    * Dumps an object into DynamoDB
    *
    * If the object is new, then this creates the item in DynamoDB,
    * otherwise it overwrites the exisiting item.
    *
    * @tparam T
    *     the type of the object to put.
    * @param obj
    *     the object to put.
    * @param serializer
    *     an implicit object serializer.
    */
  def dump[T](
    obj: T
  )(implicit serializer: DynamoDBSerializer[T]): Future[Unit] = {
    val request =
      new PutItemRequest()
      .withTableName(tableName)
      .withItem(serializer.toAttributeMap(obj).asJava)
    if (logger.isDebugEnabled)
      request.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

    client.putItem(request) map { result =>
      if (logger.isDebugEnabled)
        logger.debug(s"dump() ConsumedCapacity = ${result.getConsumedCapacity()}")
    }
  }



  /**
    * A method overloading container for [[loadByKey]].
    *
    * This class contains the two overloaded implementations
    * of [[loadByKey]].
    *
    * @tparam T
    *     the type of the object to be loaded.
    * @see [[loadByKey]]
    */
  class LoadByKey[T] {

    /**
      * Load an object by its hash key.
      *
      * If the item is not found in the DynamoDB table,
      * then None is returned.
      *
      * @tparam K
      *     a type that is viewable as an [[AttributeValue]].
      * @param hashKey
      *     the hash key of the object to retrieve.
      * @param serializer
      *     an implicit object serializer.
      * @return the retreived object, or None, in a future.
      * @see [[loadByKey]]
      */
    def apply[K <% AttributeValue]
             (hashKey: K)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Option[T]] = {
      val request =
        new GetItemRequest()
        .withTableName(tableName)
        .withKey(serializer.makeKey(hashKey).asJava)
        .withConsistentRead(config.consistentReads)
      if (logger.isDebugEnabled)
        request.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

      client.getItem(request) map { result =>
        if (logger.isDebugEnabled)
          logger.debug(s"loadByKey() ConsumedCapacity = ${result.getConsumedCapacity()}")

        Option { result.getItem } map { item =>
          serializer.fromAttributeMap(item.asScala)
        }
      }
    }

    /**
      * Load an object by its hash key and range key.
      *
      * If the item is not found in the DynamoDB table,
      * then None is returned.
      *
      * @tparam K1
      *     a type that is viewable as an [[AttributeValue]].
      * @tparam K2
      *     a type that is viewable as an [[AttributeValue]].
      * @param hashKey
      *     the hash key of the object to retrieve.
      * @param rangeKey
      *     the range key of the object to retrieve.
      * @param serializer
      *     an implicit object serializer.
      * @return the retreived object, or None, in a future.
      * @see [[loadByKey]]
      */
    def apply[K1 <% AttributeValue, K2 <% AttributeValue]
             (hashKey: K1, rangeKey: K2)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Option[T]] = {
      val request =
        new GetItemRequest()
        .withTableName(tableName)
        .withKey(serializer.makeKey(hashKey, rangeKey).asJava)
        .withConsistentRead(config.consistentReads)
      if (logger.isDebugEnabled)
        request.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

      client.getItem(request) map { result =>
        if (logger.isDebugEnabled)
          logger.debug(s"loadByKey() ConsumedCapacity = ${result.getConsumedCapacity()}")

        Option { result.getItem } map { item =>
          serializer.fromAttributeMap(item.asScala)
        }
      }
    }
  }

  /**
    * Load an object by its hash key (and range key).
    *
    * If the item is not found in the DynamoDB table,
    * then None is returned.
    *
    * @tparam T
    *     the type of the object to be loaded.
    * @see [[LoadByKey]]
    */
  def loadByKey[T] = new LoadByKey[T]



  /**
    * Scan a table.
    *
    * This method will internally make repeated scan calls
    * until the full result of the scan has been retrieved.
    *
    * @param scanFilter
    *     the optional filter conditions for the scan.
    * @return sequence of scanned objects in a future.
    * @see [[countScan]]
    */
  def scan[T](
    scanFilter: Map[String, Condition] = Map.empty
  )(implicit serializer: DynamoDBSerializer[T]): Future[Seq[T]] = {
    val scanRequest =
      new ScanRequest()
      .withTableName(tableName)
      .withScanFilter(scanFilter.asJava)
    if (logger.isDebugEnabled)
      scanRequest.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

    val builder = Seq.newBuilder[T]

    def local(lastKey: Option[DynamoDBKey] = None): Future[Unit] =
      client.scan(
        scanRequest.withExclusiveStartKey(lastKey.orNull)
      ) flatMap { result =>
        if (logger.isDebugEnabled)
          logger.debug(s"scan() ConsumedCapacity = ${result.getConsumedCapacity()}")

        builder ++= result.getItems.asScala.view map { item =>
          serializer.fromAttributeMap(item.asScala)
        }

        Option { result.getLastEvaluatedKey } match {
          case None   => Future.successful(())
          case optKey => local(optKey)
        }
      }

    local() map { _ => builder.result }
  }

  /**
    * Scan a table.
    *
    * This method will issue one scan request, stopping either
    * at the supplied limit or at the response size limit.
    *
    * @param scanFilter
    *     the optional filter conditions for the scan.
    * @param limit
    *     the optional limit for the number of items to return.
    * @return sequence of scanned objects in a future.
    * @see [[countScan]]
    */
  def scanOnce[T](
    scanFilter: Map[String, Condition] = Map.empty,
    limit:      Int                    = 0
  )(implicit serializer: DynamoDBSerializer[T]): Future[Seq[T]] = {
    val scanRequest =
      new ScanRequest()
      .withTableName(tableName)
      .withScanFilter(scanFilter.asJava)
    if (limit > 0)
      scanRequest.setLimit(limit)
    if (logger.isDebugEnabled)
      scanRequest.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

    client.scan(scanRequest) map { result =>
      if (logger.isDebugEnabled)
        logger.debug(s"scanOnce() ConsumedCapacity = ${result.getConsumedCapacity()}")

      result.getItems.asScala.view map { item =>
        serializer.fromAttributeMap(item.asScala)
      }
    }
  }



  /**
    * Scan a table and return a count.
    *
    * This method will internally make repeated scan calls
    * until the full result of the scan has been retrieved.
    *
    * @param scanFilter
    *     the optional filter conditions for the scan
    * @return the total number of scanned items in a future
    * @see [[scan]]
    */
  def countScan[T](
    scanFilter: Map[String, Condition] = Map.empty
  )(implicit serializer: DynamoDBSerializer[T]): Future[Long] = {
    val scanRequest =
      new ScanRequest()
      .withTableName(tableName)
      .withScanFilter(scanFilter.asJava)
      .withSelect(Select.COUNT)
    if (logger.isDebugEnabled)
      scanRequest.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

    def local(count: Long = 0L, lastKey: Option[DynamoDBKey] = None): Future[Long] =
      client.scan(
        scanRequest.withExclusiveStartKey(lastKey.orNull)
      ) flatMap { result =>
        if (logger.isDebugEnabled)
          logger.debug(s"countScan() ConsumedCapacity = ${result.getConsumedCapacity()}")

        val newCount = count + result.getCount
        Option { result.getLastEvaluatedKey } match {
          case None   => Future.successful(newCount)
          case optKey => local(newCount, optKey)
        }
      }

    local()
  }



  /**
    * A method overloading container for [[query]].
    *
    * This class contains the overloaded implementations of [[query]].
    *
    * @tparam T
    *     the type of the object returned by the query.
    * @see [[query]]
    */
  class Query[T] {

    /**
      * Query a table.
      *
      * This is the most primitive overload, which takess a raw
      * query request object.
      *
      * This method will internally make repeated query calls
      * until the full result of the query has been retrieved.
      *
      * @param queryRequest
      *     the query request object.
      * @param serializer
      *     an implicit object serializer.
      * @return result sequence of the query in a future.
      * @see [[query]]
      * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/QueryRequest.html QueryRequest]]
      */
    def apply(queryRequest: QueryRequest)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Seq[T]] =
      queryRaw(queryRequest)

    /**
      * Query a table, with a limit.
      *
      * This is the most primitive overload, which takess a raw
      * query request object.
      *
      * This method will internally make repeated query calls
      * until at most the given limit has been retrieved.
      *
      * @param queryRequest
      *     the query request object.
      * @param totalLimit
      *     the total number of results you want.
      * @param serializer
      *     an implicit object serializer.
      * @return result sequence of the query in a future.
      * @see [[query]]
      * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/QueryRequest.html QueryRequest]]
      */
    def apply(queryRequest: QueryRequest, totalLimit: Int)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Seq[T]] =
      queryRaw(queryRequest, Some(totalLimit))

    private def queryRaw(queryRequest: QueryRequest, totalLimit: Option[Int] = None)
                        (implicit serializer: DynamoDBSerializer[T])
                        : Future[Seq[T]] = {
      // note this mutates the query request
      queryRequest
        .withTableName(tableName)
        .withConsistentRead(config.consistentReads)
      if (logger.isDebugEnabled)
        queryRequest.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

      val builder = Seq.newBuilder[T]

      def local(lastKey: Option[DynamoDBKey] = None, numberLeftToFetch: Option[Int] = None): Future[Unit] =
        client.query(
          queryRequest.withExclusiveStartKey(lastKey.orNull)
        ) flatMap { result =>
          if (logger.isDebugEnabled)
            logger.debug(s"query() ConsumedCapacity = ${result.getConsumedCapacity}")

          val queryResult = result.getItems.asScala map { item =>
              serializer.fromAttributeMap(item.asScala)
          }

          val optKey = Option { result.getLastEvaluatedKey }
          numberLeftToFetch match {
            case None =>
              builder ++= queryResult
              if (optKey.isEmpty)
                Future.successful(())
              else
                local(optKey, None)
            case Some(n) =>
              if (n <= queryResult.size || optKey.isEmpty) {
                builder ++= queryResult.take(n)
                Future.successful(())
              } else {
                builder ++= queryResult
                local(optKey, Some(n - queryResult.size))
              }
          }
        }

      local(None, totalLimit) map { _ => builder.result() }
    }


    /**
      * Query a table by a hash key value.
      *
      * The result will be all items with the same hash key
      * value, but varying range keys.
      *
      * This method will internally make repeated query calls
      * until the full result of the query has been retrieved.
      *
      * @tparam K
      *     a type that is viewable as an [[AttributeValue]].
      * @param hashValue
      *     the hash key value to match.
      * @param serializer
      *     an implicit object serializer.
      * @return result sequence of the query in a future.
      * @see [[query]]
      */
    def apply[K <% AttributeValue]
             (hashValue: K)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Seq[T]] =
      queryOnHash(hashValue)

    /**
      * Query a table by a hash key value, with a limit.
      *
      * The result will be all items with the same hash key
      * value, but varying range keys.
      *
      * This method will internally make repeated query calls
      * until at most the given limit has been retrieved.
      *
      * @tparam K
      *     a type that is viewable as an [[AttributeValue]].
      * @param hashValue
      *     the hash key value to match.
      * @param totalLimit
      *     the total number of results you want.
      * @param serializer
      *     an implicit object serializer.
      * @return result sequence of the query in a future.
      * @see [[query]]
      */
    def apply[K <% AttributeValue]
             (hashValue: K, totalLimit: Int)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Seq[T]] =
      queryOnHash(hashValue, Some(totalLimit))

    private def queryOnHash
        [K <% AttributeValue]
        (hashValue: K, totalLimit: Option[Int] = None)
        (implicit serializer: DynamoDBSerializer[T])
        : Future[Seq[T]] =
      queryRaw(mkHashKeyQuery(hashValue), totalLimit)



    /**
      * Query a table by a hash value and range condition.
      *
      * The result will be all items with the same hash key
      * value, and range keys that match the range condition.
      *
      * This method will internally make repeated query calls
      * until the full result of the query has been retrieved.
      *
      * @tparam K
      *     a type that is viewable as an [[AttributeValue]].
      * @param hashValue
      *     the hash key value to match.
      * @param rangeCondition
      *     the condition to apply to the range key.
      * @param serializer
      *     an implicit object serializer.
      * @return result sequence of the query in a future.
      * @see [[query]]
      */
    def apply[K <% AttributeValue]
             (hashValue:         K,
              rangeCondition:    Condition)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Seq[T]] =
      queryOnHashAndRange(hashValue, rangeCondition)

    /**
      * Query a table by a hash value and range condition,
      * ascending or desending.
      *
      * The result will be all items with the same hash key
      * value, and range keys that match the range condition.
      *
      * This method will internally make repeated query calls
      * until the full result of the query has been retrieved.
      *
      * @tparam K
      *     a type that is viewable as an [[AttributeValue]].
      * @param hashValue
      *     the hash key value to match.
      * @param rangeCondition
      *     the condition to apply to the range key.
      * @param scanIndexForward
      *     true for forwards scan, and false for reverse scan.
      * @param serializer
      *     an implicit object serializer.
      * @return result sequence of the query in a future.
      * @see [[query]]
      */
    def apply[K <% AttributeValue]
             (hashValue:         K,
              rangeCondition:    Condition,
              scanIndexForward:  Boolean)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Seq[T]] =
      queryOnHashAndRange(hashValue, rangeCondition, scanIndexForward)

    /**
      * Query a table by a hash value and range condition,
      * with a limit.
      *
      * The result will be all items with the same hash key
      * value, and range keys that match the range condition.
      *
      * This method will internally make repeated query calls
      * until at most the given limit has been retrieved.
      *
      * @tparam K
      *     a type that is viewable as an [[AttributeValue]].
      * @param hashValue
      *     the hash key value to match.
      * @param rangeCondition
      *     the condition to apply to the range key.
      * @param totalLimit
      *     the total number of results you want.
      * @param serializer
      *     an implicit object serializer.
      * @return result sequence of the query in a future.
      * @see [[query]]
      */
    def apply[K <% AttributeValue]
             (hashValue:         K,
              rangeCondition:    Condition,
              totalLimit:        Int)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Seq[T]] =
      queryOnHashAndRange(hashValue, rangeCondition, totalLimit = Some(totalLimit))

    /**
      * Query a table by a hash value and range condition,
      * ascending or desending, with a limit.
      *
      * The result will be all items with the same hash key
      * value, and range keys that match the range condition.
      *
      * This method will internally make repeated query calls
      * until at most the given limit has been retrieved.
      *
      * @tparam K
      *     a type that is viewable as an [[AttributeValue]].
      * @param hashValue
      *     the hash key value to match.
      * @param rangeCondition
      *     the condition to apply to the range key.
      * @param scanIndexForward
      *     true for forwards scan, and false for reverse scan.
      * @param totalLimit
      *     the total number of results you want.
      * @param serializer
      *     an implicit object serializer.
      * @return result sequence of the query in a future.
      * @see [[query]]
      */
    def apply[K <% AttributeValue]
             (hashValue:         K,
              rangeCondition:    Condition,
              scanIndexForward:  Boolean,
              totalLimit:        Int)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Seq[T]] =
      queryOnHashAndRange(hashValue, rangeCondition, scanIndexForward, Some(totalLimit))

    private def queryOnHashAndRange
        [K <% AttributeValue]
        (hashValue:         K,
         rangeCondition:    Condition,
         scanIndexForward:  Boolean     = true,
         totalLimit:        Option[Int] = None)
        (implicit serializer: DynamoDBSerializer[T])
        : Future[Seq[T]] =
      queryRaw(mkHashAndRangeKeyQuery(hashValue, rangeCondition).withScanIndexForward(scanIndexForward), totalLimit)



    /**
      * Query a secondary index by a hash value and range condition.
      *
      * This query targets a named secondary index. The index
      * being used must be named, as well well at the name of
      * the attribute used as a range key in the index.
      * The result will be all items with the same hash key
      * value, and range keys that match the range condition.
      *
      * This method will internally make repeated query calls
      * until the full result of the query has been retrieved.
      *
      * Note that all attributes will be requested, so that
      * the serializer will get a complete item. This may incur
      * extra read capacity, depending on what attributes
      * are projected into the index.
      *
      * @tparam K
      *     a type that is viewable as an [[AttributeValue]].
      * @param indexName
      *     the name of the secondary index to query.
      * @param hashValue
      *     the hash key value to match.
      * @param rangeAttributeName
      *     the name of the range key attribute used by the index.
      * @param rangeCondition
      *     the condition to apply to the range key.
      * @param serializer
      *     an implicit object serializer.
      * @return result sequence of the query in a future.
      * @see [[query]]
      */
    def apply[K <% AttributeValue]
             (indexName:           String,
              hashValue:           K,
              rangeAttributeName:  String,
              rangeCondition:      Condition)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Seq[T]] =
      queryOnSecondaryIndex(indexName, hashValue, rangeAttributeName, rangeCondition)

    /**
      * Query a secondary index by a hash value and range condition,
      * ascending or desending.
      *
      * This query targets a named secondary index. The index
      * being used must be named, as well well at the name of
      * the attribute used as a range key in the index.
      * The result will be all items with the same hash key
      * value, and range keys that match the range condition.
      *
      * This method will internally make repeated query calls
      * until the full result of the query has been retrieved.
      *
      * Note that all attributes will be requested, so that
      * the serializer will get a complete item. This may incur
      * extra read capacity, depending on what attributes
      * are projected into the index.
      *
      * @tparam K
      *     a type that is viewable as an [[AttributeValue]].
      * @param indexName
      *     the name of the secondary index to query.
      * @param hashValue
      *     the hash key value to match.
      * @param rangeAttributeName
      *     the name of the range key attribute used by the index.
      * @param rangeCondition
      *     the condition to apply to the range key.
      * @param scanIndexForward
      *     true for forwards scan, and false for reverse scan.
      * @param serializer
      *     an implicit object serializer.
      * @return result sequence of the query in a future.
      * @see [[query]]
      */
    def apply[K <% AttributeValue]
             (indexName:           String,
              hashValue:           K,
              rangeAttributeName:  String,
              rangeCondition:      Condition,
              scanIndexForward:    Boolean)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Seq[T]] =
      queryOnSecondaryIndex(indexName, hashValue, rangeAttributeName, rangeCondition, scanIndexForward)

    /**
      * Query a secondary index by a hash value and range condition,
      * with a limit.
      *
      * This query targets a named secondary index. The index
      * being used must be named, as well well at the name of
      * the attribute used as a range key in the index.
      * The result will be all items with the same hash key
      * value, and range keys that match the range condition.
      *
      * This method will internally make repeated query calls
      * until at most the given limit has been retrieved.
      *
      * Note that all attributes will be requested, so that
      * the serializer will get a complete item. This may incur
      * extra read capacity, depending on what attributes
      * are projected into the index.
      *
      * @tparam K
      *     a type that is viewable as an [[AttributeValue]].
      * @param indexName
      *     the name of the secondary index to query.
      * @param hashValue
      *     the hash key value to match.
      * @param rangeAttributeName
      *     the name of the range key attribute used by the index.
      * @param rangeCondition
      *     the condition to apply to the range key.
      * @param totalLimit
      *     the total number of results you want.
      * @param serializer
      *     an implicit object serializer.
      * @return result sequence of the query in a future.
      * @see [[query]]
      */
    def apply[K <% AttributeValue]
             (indexName:           String,
              hashValue:           K,
              rangeAttributeName:  String,
              rangeCondition:      Condition,
              totalLimit:          Int)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Seq[T]] =
      queryOnSecondaryIndex(indexName, hashValue, rangeAttributeName, rangeCondition, totalLimit = Some(totalLimit))

    /**
      * Query a secondary index by a hash value and range condition,
      * ascending or desending, with a limit.
      *
      * This query targets a named secondary index. The index
      * being used must be named, as well well at the name of
      * the attribute used as a range key in the index.
      * The result will be all items with the same hash key
      * value, and range keys that match the range condition.
      *
      * This method will internally make repeated query calls
      * until at most the given limit has been retrieved.
      *
      * Note that all attributes will be requested, so that
      * the serializer will get a complete item. This may incur
      * extra read capacity, depending on what attributes
      * are projected into the index.
      *
      * @tparam K
      *     a type that is viewable as an [[AttributeValue]].
      * @param indexName
      *     the name of the secondary index to query.
      * @param hashValue
      *     the hash key value to match.
      * @param rangeAttributeName
      *     the name of the range key attribute used by the index.
      * @param rangeCondition
      *     the condition to apply to the range key.
      * @param scanIndexForward
      *     true for forwards scan, and false for reverse scan.
      * @param totalLimit
      *     the total number of results you want.
      * @param serializer
      *     an implicit object serializer.
      * @return result sequence of the query in a future.
      * @see [[query]]
      */
    def apply[K <% AttributeValue]
             (indexName:           String,
              hashValue:           K,
              rangeAttributeName:  String,
              rangeCondition:      Condition,
              scanIndexForward:    Boolean,
              totalLimit:          Int)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Seq[T]] =
      queryOnSecondaryIndex(indexName, hashValue, rangeAttributeName, rangeCondition, scanIndexForward, Some(totalLimit))

    private def queryOnSecondaryIndex
        [K <% AttributeValue]
        (indexName:           String,
         hashValue:           K,
         rangeAttributeName:  String,
         rangeCondition:      Condition,
         scanIndexForward:    Boolean     = true,
         totalLimit:          Option[Int] = None)
        (implicit serializer: DynamoDBSerializer[T])
        : Future[Seq[T]] =
      queryRaw(
        new QueryRequest()
          .withIndexName(indexName)
          .withKeyConditions(
            Map(
              serializer.hashAttributeName -> QueryCondition.equalTo(hashValue),
              rangeAttributeName           -> rangeCondition
            ).asJava
          )
          .withSelect(Select.ALL_ATTRIBUTES)
          .withScanIndexForward(scanIndexForward),
        totalLimit
      )
  }

  /**
    * Query a table.
    *
    * This method will internally make repeated query calls
    * until the full result of the query has been retrieved,
    * or the at most the size of the limit, if specified.
    *
    * @tparam T
    *     the type of the object returned by the query.
    * @see [[Query]]
    * @see [[queryOnce]]
    * @see [[countQuery]]
    */
  def query[T] = new Query[T]


  /**
    * A method overloading container for [[queryOnce]].
    *
    * This class contains the overloaded implementations of [[queryOnce]].
    *
    * @tparam T
    *     the type of the object returned by the query.
    * @see [[queryOnce]]
    */
  class QueryOnce[T] {

    /**
      * Query a table.
      *
      * This is the most primitive overload, which takes a raw
      * query request object.
      *
      * This method will issue one query request, stopping
      * at the response size limit.
      *
      * @param queryRequest
      *     the query request object.
      * @param serializer
      *     an implicit object serializer.
      * @return result sequence of the query in a future.
      * @see [[queryOnce]]
      * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/QueryRequest.html QueryRequest]]
      */
    def apply(queryRequest: QueryRequest)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Seq[T]] = {
      // note this mutates the query request
      queryRequest
        .withTableName(tableName)
        .withConsistentRead(config.consistentReads)
      if (logger.isDebugEnabled)
        queryRequest.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

      client.query(
        queryRequest
      ) map { result =>
        if (logger.isDebugEnabled)
          logger.debug(s"queryOnce() ConsumedCapacity = ${result.getConsumedCapacity()}")

        result.getItems.asScala.view map { item =>
          serializer.fromAttributeMap(item.asScala)
        }
      }
    }

    /**
      * Query a table by a hash key value.
      *
      * The result will be all items with the same hash key
      * value, but varying range keys.
      *
      * This method will issue one query request, stopping either
      * at the supplied limit or at the response size limit.
      *
      * @tparam K
      *     a type that is viewable as an [[AttributeValue]].
      * @param hashValue
      *     the hash key value to match.
      * @param limit
      *     the optional limit for the number of items to return.
      * @param serializer
      *     an implicit object serializer.
      * @return result sequence of the query in a future.
      * @see [[queryOnce]]
      */
    def apply[K <% AttributeValue]
             (hashValue: K, limit: Int = 0)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Seq[T]] = {
      val request = mkHashKeyQuery(hashValue)
      if (limit > 0) request.setLimit(limit)
      apply(request)
    }

    /**
      * Query a table by a hash value and range condition.
      *
      * The result will be all items with the same hash key
      * value, and range keys that match the range condition.
      *
      * This method will issue one query request, stopping either
      * at the supplied limit or at the response size limit.
      *
      * @tparam K
      *     a type that is viewable as an [[AttributeValue]].
      * @param hashValue
      *     the hash key value to match.
      * @param rangeCondition
      *     the condition to apply to the range key.
      * @param scanIndexForward
      *     true (default) for forwards scan, and false for reverse scan.
      * @param limit
      *     the optional limit for the number of items to return.
      * @param serializer
      *     an implicit object serializer.
      * @return result sequence of the query in a future.
      * @see [[queryOnce]]
      */
    def apply[K <% AttributeValue]
             (hashValue:         K,
              rangeCondition:    Condition,
              scanIndexForward:  Boolean    = true,
              limit:             Int        = 0)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Seq[T]] = {
      val request =
        mkHashAndRangeKeyQuery(hashValue, rangeCondition)
        .withScanIndexForward(scanIndexForward)
      if (limit > 0) request.setLimit(limit)
      apply(request)
    }

    /**
      * Query a secondary index by a hash value and range condition.
      *
      * This query targets a named secondary index. The index
      * being used must be named, as well well at the name of
      * the attribute used as a range key in the index.
      * The result will be all items with the same hash key
      * value, and range keys that match the range condition.
      *
      * This method will issue one query request, stopping either
      * at the supplied limit or at the response size limit.
      *
      * Note that all attributes will be requested, so that
      * the serializer will get a complete item. This may incur
      * extra read capacity, depending on what attributes
      * are projected into the index.
      *
      * @tparam K
      *     a type that is viewable as an [[AttributeValue]].
      * @param indexName
      *     the name of the secondary index to query.
      * @param hashValue
      *     the hash key value to match.
      * @param rangeAttributeName
      *     the name of the range key attribute used by the index.
      * @param rangeCondition
      *     the condition to apply to the range key.
      * @param scanIndexForward
      *     true (default) for forwards scan, and false for reverse scan.
      * @param limit
      *     the optional limit for the number of items to return.
      * @param serializer
      *     an implicit object serializer.
      * @return result sequence of the query in a future.
      * @see [[queryOnce]]
      */
    def apply[K <% AttributeValue]
             (indexName:           String,
              hashValue:           K,
              rangeAttributeName:  String,
              rangeCondition:      Condition,
              scanIndexForward:    Boolean    = true,
              limit:               Int        = 0)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Seq[T]] = {
      val request =
        new QueryRequest()
          .withIndexName(indexName)
          .withKeyConditions(
            Map(
              serializer.hashAttributeName -> QueryCondition.equalTo(hashValue),
              rangeAttributeName           -> rangeCondition
            ).asJava
          )
          .withSelect(Select.ALL_ATTRIBUTES)
          .withScanIndexForward(scanIndexForward)
      if (limit > 0) request.setLimit(limit)
      apply(request)
    }
  }

  /**
    * Query a table.
    *
    * This method will issue one query request, stopping either
    * at the supplied limit or at the response size limit.
    *
    * @tparam T
    *     the type of the object returned by the query.
    * @see [[QueryOnce]]
    * @see [[query]]
    * @see [[countQuery]]
    */
  def queryOnce[T] = new QueryOnce[T]


  /**
    * A method overloading container for [[countQuery]].
    *
    * This class contains the overloaded implementations of [[countQuery]].
    *
    * @tparam T
    *     the type of object queried.
    * @see [[countQuery]]
    */
  class CountQuery[T] {

    /**
      * Query a table, counting the results.
      *
      * This is the most primitive overload, which takes a raw
      * query request object.
      *
      * This method will internally make repeated query calls
      * until the full result of the query has been retrieved.
      *
      * @param queryRequest
      *     the query request object.
      * @param serializer
      *     an implicit object serializer.
      * @return the total number items that match the query in a future.
      * @see [[countQuery]]
      * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/QueryRequest.html QueryRequest]]
      */
    def apply(queryRequest: QueryRequest)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Long] = {
      // note this mutates the query request
      queryRequest
        .withTableName(tableName)
        .withSelect(Select.COUNT)
        .withConsistentRead(config.consistentReads)
      if (logger.isDebugEnabled)
        queryRequest.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

      def local(count: Long = 0L, lastKey: Option[DynamoDBKey] = None): Future[Long] =
        client.query(
          queryRequest.withExclusiveStartKey(lastKey.orNull)
        ) flatMap { result =>
          if (logger.isDebugEnabled)
            logger.debug(s"countQuery() ConsumedCapacity = ${result.getConsumedCapacity()}")

          val newCount = count + result.getCount

          Option { result.getLastEvaluatedKey } match {
            case None   => Future.successful(newCount)
            case optKey => local(newCount, optKey)
          }
        }

      local()
    }

    /**
      * Query a table by a hash key value, counting the results.
      *
      * The result will be the count of all items with the
      * same hash key value, but varying range keys.
      *
      * This method will internally make repeated query calls
      * until the full result of the query has been retrieved.
      *
      * @tparam K
      *     a type that is viewable as an [[AttributeValue]].
      * @param hashValue
      *     the hash key value to match.
      * @param serializer
      *     an implicit object serializer.
      * @return the total number items that match the query in a future.
      * @see [[countQuery]]
      */
    def apply[K <% AttributeValue]
             (hashValue: K)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Long] =
      apply(mkHashKeyQuery(hashValue))

    /**
      * Query a table by a hash value and range condition,
      * counting the results.
      *
      * The result will be the count of all items with
      * the same hash key value, and range keys that
      * match the range condition.
      *
      * This method will internally make repeated query calls
      * until the full result of the query has been retrieved.
      *
      * @tparam K
      *     a type that is viewable as an [[AttributeValue]].
      * @param hashValue
      *     the hash key value to match.
      * @param rangeCondition
      *     the condition to apply to the range key.
      * @param serializer
      *     an implicit object serializer.
      * @return the total number items that match the query in a future.
      * @see [[countQuery]]
      */
    def apply[K <% AttributeValue]
             (hashValue: K, rangeCondition: Condition)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Long] =
      apply(mkHashAndRangeKeyQuery(hashValue, rangeCondition))

    /**
      * Query a secondary index by a hash value and range
      * condition, counting the results.
      *
      * This query targets a named secondary index. The index
      * being used must be named, as well well at the name of
      * the attribute used as a range key in the index.
      * The result will be all the count of all items with the
      * same hash key value, and range keys that match the
      * range condition.
      *
      * This method will internally make repeated query calls
      * until the full result of the query has been retrieved.
      *
      * @tparam K
      *     a type that is viewable as an [[AttributeValue]].
      * @param indexName
      *     the name of the secondary index to query.
      * @param hashValue
      *     the hash key value to match.
      * @param rangeAttributeName
      *     the name of the range key attribute used by the index.
      * @param rangeCondition
      *     the condition to apply to the range key.
      * @param serializer
      *     an implicit object serializer.
      * @return the total number items that match the query in a future.
      * @see [[countQuery]]
      */
    def apply[K <% AttributeValue]
             (indexName: String, hashValue: K, rangeAttributeName: String, rangeCondition: Condition)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Long] =
      apply(
        new QueryRequest()
          .withIndexName(indexName)
          .withKeyConditions(
            Map(
              serializer.hashAttributeName -> QueryCondition.equalTo(hashValue),
              rangeAttributeName           -> rangeCondition
            ).asJava
          )
      )
  }

  /**
    * Query a table, counting the results.
    *
    * This method will internally make repeated query calls
    * until the full result of the query has been retrieved.
    *
    * @tparam T
    *     the type of object queried.
    * @see [[CountQuery]]
    * @see [[query]]
    * @see [[queryOnce]]
    */
  def countQuery[T] = new CountQuery[T]



  /**
    * Helper method to build seqences of keys
    *
    * Turn a sequence of hash values, into a sequence of hash keys;
    * or turn a sequence of hash values and a sequence of range values,
    * into a sequence of hash+range keys.
    *
    * @tparam T
    *     the type object for the serializer.
    * @tparam K1
    *     a type that is viewable as an [[AttributeValue]].
    * @tparam K2
    *     a type that is viewable as an [[AttributeValue]].
    * @param hashKeys
    *     a sequence of hash key values.
    * @param rangeKeys
    *     an optional sequence of range key values.
    * @param serializer
    *     an implicit object serializer.
    * @return a sequence of DynamoDB keys (a map of strings to values).
    * @throws IllegalArgumentException if the sequence of hash key values is empty.
    * @throws IllegalArgumentException if the number of hash and range keys don't match.
    */
  private def zipKeySeqs[T, K1 <% AttributeValue, K2 <% AttributeValue]
                        (hashKeys:  Seq[K1], rangeKeys: Seq[K2] = Seq.empty)
                        (implicit serializer: DynamoDBSerializer[T])
                        : Seq[DynamoDBKey] =
    if (hashKeys.isEmpty) {
      throw new IllegalArgumentException("AmazonDynamoDBScalaMapper: no hash keys given")
    } else if (!rangeKeys.isEmpty && (hashKeys.length != rangeKeys.length)) {
      throw new IllegalArgumentException("AmazonDynamoDBScalaMapper: the number of hash and range keys don't match")
    } else if (rangeKeys.isEmpty) {
      hashKeys map { hashKey =>
        serializer.makeKey(hashKey).asJava
      }
    } else {
      (hashKeys, rangeKeys).zipped map { case (hashKey, rangeKey) =>
        serializer.makeKey(hashKey, rangeKey).asJava
      }
    }



  /**
    * A method overloading container for [[batchLoadByKeys]].
    *
    * This class contains the overloaded implementation of [[batchLoadByKeys]].
    *
    * @tparam T
    *     the type of the objects returned by the batch load.
    * @see [[batchLoadByKeys]]
    */
  class BatchLoadByKeys[T] {

    /**
      * Load a sequence of objects by a sequence of hash key values.
      *
      * This method will internally make repeated batchGetItem
      * calls, with up to 25 keys at a time, until all of the
      * given keys have been requested.
      *
      * @tparam K
      *     a type that is viewable as an [[AttributeValue]].
      * @param hashKeys
      *     the hash key values of the objects to retrieve.
      * @param serializer
      *     an implicit object serializer.
      * @return sequence of retrieved objects in a future.
      * @see [[batchLoadByKeys]]
      */
    def apply[K <% AttributeValue]
             (hashKeys:  Seq[K])
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Seq[T]] =
      apply(hashKeys, Seq.empty[String])
      /*
       * Seq.empty[String] ensures that a valid view is inferred
       * otherwise we will get an implicit error if Seq.empty
       * is given type Seq[Nothing].
       */

    /**
      * Load a sequence of objects by a sequence of hash key
      * values and a sequences of range key values.
      *
      * This method will internally make repeated batchGetItem
      * calls, with up to 25 keys at a time, until all of the
      * given keys have been requested.
      *
      * @tparam K1
      *     a type that is viewable as an [[AttributeValue]].
      * @tparam K2
      *     a type that is viewable as an [[AttributeValue]].
      * @param hashKeys
      *     the hash key values of the objects to retrieve.
      * @param rangeKeys
      *     the range key values of the objects to retrieve.
      * @param serializer
      *     an implicit object serializer.
      * @return sequence of retrieved objects in a future.
      * @see [[batchLoadByKeys]]
      */
    def apply[K1 <% AttributeValue, K2 <% AttributeValue]
             (hashKeys:  Seq[K1], rangeKeys: Seq[K2] = Seq.empty)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Seq[T]] = {
      val keys: Seq[DynamoDBKey] = zipKeySeqs(hashKeys, rangeKeys)
      val builder = Seq.newBuilder[T]

      def local(keys: (Seq[DynamoDBKey], Seq[DynamoDBKey])): Future[Unit] = {
        val request =
          new BatchGetItemRequest()
          .withRequestItems(
            Map(
              tableName ->
                new KeysAndAttributes()
                .withKeys(
                  keys._1.asJavaCollection
                )
                .withConsistentRead(config.consistentReads)
            ).asJava
          )
        if (logger.isDebugEnabled)
          request.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

        client.batchGetItem(request) flatMap { result =>
          if (logger.isDebugEnabled)
            logger.debug(s"batchLoadByKeys() ConsumedCapacity = ${result.getConsumedCapacity()}")

          builder ++= result.getResponses.get(tableName).asScala.view map { item =>
            serializer.fromAttributeMap(item.asScala)
          }

          if (keys._2.isEmpty)
            Future.successful(())
          else
            local(keys._2.splitAt(100))
        }
      }

      local(keys.splitAt(100)) map { _ => builder.result }
    }
  }

  /**
    * Load a sequence of objects by a sequence of keys.
    *
    * This method will internally make repeated batchGetItem
    * calls, with up to 25 keys at a time, until all of the
    * given keys have been requested.
    *
    * @tparam T
    *     the type of the objects returned by the batch load.
    * @see [[BatchLoadByKeys]]
    */
  def batchLoadByKeys[T] = new BatchLoadByKeys[T]



  /**
    * A helper method to check for and retry any unprocessed items in
    * a batch write result.
    *
    * This method will attempt to retry any portion of a failed batch write.
    *
    * @param lastResult
    *     the result object from a batchWrite operation.
    */
  private def checkRetryBatchWrite(lastResult: BatchWriteItemResult): Future[Unit] = {
    val retryItems = lastResult.getUnprocessedItems
    if (retryItems.isEmpty)
      Future.successful(())
    else {
      val request =
        new BatchWriteItemRequest()
        .withRequestItems(retryItems)
      if (logger.isDebugEnabled)
        request.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

      client.batchWriteItem(request) flatMap { result =>
        if (logger.isDebugEnabled)
          logger.debug(s"checkRetryBatchWrite() ConsumedCapacity = ${result.getConsumedCapacity()}")

        checkRetryBatchWrite(result)
      }
    }
  }



  /**
    * Dump a sequence of objects into DynamoDB
    *
    * This method will internally make repeated batchWriteItem
    * calls, with up to 25 objects at a time, until all the input
    * objects have been written.
    *
    * Objects that are new will create new items in DynamoDB,
    * otherwise they will overwrite exisiting items.
    *
    * @param objs
    *     the sequence of objects to write to DynamoDB.
    */
  def batchDump[T](objs: Seq[T])(implicit serializer: DynamoDBSerializer[T]): Future[Unit] = {
    def local(objsPair: (Seq[T], Seq[T])): Future[Unit] = {
      val request =
        new BatchWriteItemRequest()
        .withRequestItems(
          Map(
            tableName -> objsPair._1.view.map { obj =>
              new WriteRequest()
              .withPutRequest(
                new PutRequest()
                .withItem(serializer.toAttributeMap(obj).asJava)
              )
            } .asJava
          ).asJava
        )
      if (logger.isDebugEnabled)
        request.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

      client.batchWriteItem(request) flatMap { result =>
        if (logger.isDebugEnabled)
          logger.debug(s"batchDump() ConsumedCapacity = ${result.getConsumedCapacity()}")

        checkRetryBatchWrite(result) flatMap { _ =>
          if (objsPair._2.isEmpty)
            Future.successful(())
          else
            local(objsPair._2.splitAt(25))
        }
      }
    }

    local(objs.splitAt(25))
  }



  /**
    * Delete a sequence of objects.
    *
    * This method will internally make repeated batchWriteItem
    * calls, with up to 25 objects at a time, until all the input
    * objects have been deleted.
    *
    * @tparam T
    *     the type of objects to delete.
    * @param objs
    *     a sequence of objects to delete.
    * @param serializer
    *     an implicit object serializer.
    */
  def batchDelete[T](objs: Seq[T])(implicit serializer: DynamoDBSerializer[T]): Future[Unit] = {
    def local(objsPair: (Seq[T], Seq[T])): Future[Unit] = {
      val request =
        new BatchWriteItemRequest()
        .withRequestItems(
          Map(
            tableName -> objsPair._1.view.map { obj =>
              new WriteRequest()
              .withDeleteRequest(
                new DeleteRequest()
                .withKey(serializer.primaryKeyOf(obj).asJava)
              )
            } .asJava
          ).asJava
        )
      if (logger.isDebugEnabled)
        request.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

      client.batchWriteItem(request) flatMap { result =>
        if (logger.isDebugEnabled)
          logger.debug(s"batchDelete() ConsumedCapacity = ${result.getConsumedCapacity()}")

        checkRetryBatchWrite(result) flatMap { _ =>
          if (objsPair._2.isEmpty)
            Future.successful(())
          else
            local(objsPair._2.splitAt(25))
        }
      }
    }

    local(objs.splitAt(25))
  }



  /**
    * A method overloading container for [[batchDeleteByKeys]].
    *
    * This class contains the overloaded implementation of [[batchDeleteByKeys]].
    *
    * @tparam T
    *     the type of the objects deleted by the batch delete.
    * @see [[batchDeleteByKeys]]
    */
  class BatchDeleteByKeys[T] {

    /**
      * Delete items by a sequence of hash key values.
      *
      * This method will internally make repeated batchWriteItem
      * calls, with up to 25 keys at a time, until all the input
      * keys have been deleted.
      *
      * @tparam K
      *     a type that is viewable as an [[AttributeValue]].
      * @param hashKeys
      *     the hash key values of the items to delete.
      * @param serializer
      *     an implicit object serializer.
      * @see [[batchDeleteByKeys]]
      */
    def apply[K <% AttributeValue]
             (hashKeys: Seq[K])
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Unit] =
      apply(hashKeys, Seq.empty[String])
      /*
       * Seq.empty[String] ensures that a valid view is inferred
       * otherwise we will get an implicit error if Seq.empty
       * is given type Seq[Nothing].
       */

    /**
      * Delete items by a sequence of hash key values and a
      * sequence of range key values.
      *
      * This method will internally make repeated batchWriteItem
      * calls, with up to 25 keys at a time, until all the input
      * keys have been deleted.
      *
      * @tparam K1
      *     a type that is viewable as an [[AttributeValue]].
      * @tparam K2
      *     a type that is viewable as an [[AttributeValue]].
      * @param hashKeys
      *     the hash key values of the items to delete.
      * @param rangeKeys
      *     the range key values of the items to delete.
      * @param serializer
      *     an implicit object serializer.
      * @see [[batchDeleteByKeys]]
      */
    def apply[K1 <% AttributeValue, K2 <% AttributeValue]
             (hashKeys: Seq[K1], rangeKeys: Seq[K2] = Seq.empty)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Unit] = {
      val keys: Seq[DynamoDBKey] = zipKeySeqs(hashKeys, rangeKeys)

      def local(keysPair: (Seq[DynamoDBKey], Seq[DynamoDBKey])): Future[Unit] = {
        val request =
          new BatchWriteItemRequest()
          .withRequestItems(
            Map(
              tableName -> keysPair._1.view.map { key =>
                new WriteRequest()
                .withDeleteRequest(
                  new DeleteRequest()
                  .withKey(key)
                )
              } .asJava
            ).asJava
          )
        if (logger.isDebugEnabled)
          request.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

        client.batchWriteItem(request) flatMap { result =>
          if (logger.isDebugEnabled)
            logger.debug(s"batchDeleteByKeys() ConsumedCapacity = ${result.getConsumedCapacity()}")

          checkRetryBatchWrite(result) flatMap { _ =>
            if (keysPair._2.isEmpty)
              Future.successful(())
            else
              local(keysPair._2.splitAt(25))
          }
        }
      }

      local(keys.splitAt(25))
    }
  }

  /**
    * Delete items by a sequence of keys.
    *
    * This method will internally make repeated batchWriteItem
    * calls, with up to 25 keys at a time, until all the input
    * keys have been deleted.
    *
    * @tparam T
    *     the type of the objects deleted by the batch delete.
    * @see [[BatchDeleteByKeys]]
    */
  def batchDeleteByKeys[T] = new BatchDeleteByKeys[T]

}



/**
  * A factory for [[AmazonDynamoDBScalaMapper]].
  */
object AmazonDynamoDBScalaMapper {

  /**
    * A factory method for [[AmazonDynamoDBScalaMapper]].
    *
    * Build a new mapper from a client, a config, and
    * an execution context.
    *
    * @param dynamoClient
    *     the DynamoDB client to use.
    * @param mapperConfig
    *     the mapping configuration to use.
    * @param exec
    *     the execution context to use.
    * @return a new mapper.
    */
  def apply(
    dynamoClient: AmazonDynamoDBScalaClient,
    mapperConfig: AmazonDynamoDBScalaMapperConfig = AmazonDynamoDBScalaMapperConfig.Default
  )(implicit exec: ExecutionContext) = new AmazonDynamoDBScalaMapper {
    override val client = dynamoClient
    override protected val execCtx = exec
    override protected val config = mapperConfig
  }
}
