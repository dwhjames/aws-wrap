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
package dynamodb

import scala.language.implicitConversions

import scala.concurrent.{Future, ExecutionContext}
import scala.collection.JavaConverters._
import scala.collection.mutable

import java.util.{Map => JMap}

import com.amazonaws.services.dynamodbv2.model._

import org.slf4j.Logger
import org.slf4j.LoggerFactory

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
    *     An value that is convertable to an [[com.github.dwhjames.awswrap.dynamodb.AttributeValue AttributeValue]].
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
    *     An value that is convertable to an [[com.github.dwhjames.awswrap.dynamodb.AttributeValue AttributeValue]].
    * @param rangeKey
    *     An value that is convertable to an [[com.github.dwhjames.awswrap.dynamodb.AttributeValue AttributeValue]].
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
  trait DeleteByKeyMagnet[T] {
    def apply(): Future[Option[T]]
  }

  object DeleteByKeyMagnet {

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
    implicit def deleteByHashKey
        [T, K]
        (hashKey: K)
        (implicit serializer: DynamoDBSerializer[T], ev: K => AttributeValue)
        : DeleteByKeyMagnet[T] = new DeleteByKeyMagnet[T] { def apply() = {
      val request =
        new DeleteItemRequest()
        .withTableName(tableName)
        .withKey(serializer.makeKey(hashKey).asJava)
        .withReturnValues(ReturnValue.ALL_OLD)
      if (logger.isDebugEnabled)
        request.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

      client.deleteItem(request) map { result =>
        if (logger.isDebugEnabled)
          logger.debug(s"table $tableName deleteByKey() ConsumedCapacity = ${result.getConsumedCapacity()}")

        Option { result.getAttributes } map { item =>
          serializer.fromAttributeMap(item.asScala)
        }
      }
    }}

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
    implicit def deletebyHashAndRangeKey
        [T, K1, K2]
        (tuple: /* hashKey  */ (K1,
                /* rangeKey */  K2))
        (implicit serializer: DynamoDBSerializer[T],
         ev1: K1 => AttributeValue, ev2: K2 => AttributeValue)
        : DeleteByKeyMagnet[T] = new DeleteByKeyMagnet[T] { def apply() = {
      val request =
        new DeleteItemRequest()
        .withTableName(tableName)
        .withKey(serializer.makeKey(tuple._1, tuple._2).asJava)
        .withReturnValues(ReturnValue.ALL_OLD)
      if (logger.isDebugEnabled)
        request.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

      client.deleteItem(request) map { result =>
        if (logger.isDebugEnabled)
          logger.debug(s"table $tableName deleteByKey() ConsumedCapacity = ${result.getConsumedCapacity()}")

        Option { result.getAttributes } map { item =>
          serializer.fromAttributeMap(item.asScala)
        }
      }
    }}
  }

  /**
    * Delete a DynamoDB item by a hash key (and range key).
    *
    * If an item with the given key was deleted then the
    * deleted object returned, otherwise none.
    *
    * @tparam T
    *     the type of the object to be deleted.
    * @see [[DeleteByKeyMagnet]]
    */
  def deleteByKey[T](magnet: DeleteByKeyMagnet[T]): Future[Option[T]] = magnet()



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
        logger.debug(s"table $tableName delete() ConsumedCapacity = ${result.getConsumedCapacity()}")
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
        logger.debug(s"table $tableName dump() ConsumedCapacity = ${result.getConsumedCapacity()}")
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
  trait LoadByKeyMagnet[T] {
    def apply(): Future[Option[T]]
  }

  object LoadByKeyMagnet {

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
    implicit def loadByHashKey
        [T, K]
        (hashKey: K)
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : LoadByKeyMagnet[T] = new LoadByKeyMagnet[T] { def apply() = {
      val request =
        new GetItemRequest()
        .withTableName(tableName)
        .withKey(serializer.makeKey(hashKey).asJava)
        .withConsistentRead(config.consistentReads)
      if (logger.isDebugEnabled)
        request.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

      client.getItem(request) map { result =>
        if (logger.isDebugEnabled)
          logger.debug(s"table $tableName loadByKey() ConsumedCapacity = ${result.getConsumedCapacity()}")

        Option { result.getItem } map { item =>
          serializer.fromAttributeMap(item.asScala)
        }
      }
    }}

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
    implicit def loadByHashAndRangeKey
        [T, K1, K2]
        (tuple: /* hashKey  */ (K1,
                /* rangeKey */  K2))
        (implicit serializer: DynamoDBSerializer[T],
                  ev1: K1 => AttributeValue,
                  ev2: K2 => AttributeValue)
        : LoadByKeyMagnet[T] = new LoadByKeyMagnet[T] { def apply() = {
      val request =
        new GetItemRequest()
        .withTableName(tableName)
        .withKey(serializer.makeKey(tuple._1, tuple._2).asJava)
        .withConsistentRead(config.consistentReads)
      if (logger.isDebugEnabled)
        request.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

      client.getItem(request) map { result =>
        if (logger.isDebugEnabled)
          logger.debug(s"table $tableName loadByKey() ConsumedCapacity = ${result.getConsumedCapacity()}")

        Option { result.getItem } map { item =>
          serializer.fromAttributeMap(item.asScala)
        }
      }
    }}
  }

  /**
    * Load an object by its hash key (and range key).
    *
    * If the item is not found in the DynamoDB table,
    * then None is returned.
    *
    * @tparam T
    *     the type of the object to be loaded.
    * @see [[LoadByKeyMagnet]]
    */
  def loadByKey[T](magnet: LoadByKeyMagnet[T]): Future[Option[T]] = magnet()



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
    val builder = Seq.newBuilder[T]

    def local(lastKey: Option[DynamoDBKey] = None): Future[Seq[T]] =
      scanProgressively(scanFilter, lastEvaluatedKey = lastKey).flatMap {
        case (key, result) =>
          builder ++= result
          key match {
            case None   => Future.successful(builder.result())
            case optKey => local(optKey)
          }
      }

    local()
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
    scanProgressively(scanFilter, limit).map(_._2)
  }

  /**
    * Scan a table.
    *
    * This method will issue one scan request, stopping either
    * at the supplied limit or at the response size limit.
    *
    * @param scanFilter
    *     the optional filter conditions for the scan.
    * @param lastEvaluatedKey
    *     the optional starting key.
    * @param limit
    *     the optional limit for the number of items to return.
    * @return Tuple of
    *         some last dynamoDB key or none if the "end" of the scan is reached
    *         and sequence of scanned objects in a future.
    * @see [[countScan]]
    */
  def scanProgressively[T](
    scanFilter: Map[String, Condition] = Map.empty,
    limit: Int = 0,
    lastEvaluatedKey: Option[DynamoDBKey] = None
  )(implicit serializer: DynamoDBSerializer[T]): Future[(Option[DynamoDBKey], Seq[T])] = {
    val scanRequest =
      new ScanRequest()
        .withTableName(tableName)
        .withScanFilter(scanFilter.asJava)

    if (limit > 0)
      scanRequest.setLimit(limit)

    if (logger.isDebugEnabled)
      scanRequest.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

    client.scan(
      scanRequest.withExclusiveStartKey(lastEvaluatedKey.orNull)
    ) map { result =>
      if (logger.isDebugEnabled)
        logger.debug(s"table $tableName scanOnce() ConsumedCapacity = ${result.getConsumedCapacity()}")

      val r = result.getItems.asScala map { item =>
        serializer.fromAttributeMap(item.asScala)
      }

      val lastEvaluatedKey = Option {
        result.getLastEvaluatedKey
      }
      (lastEvaluatedKey, r)
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
          logger.debug(s"table $tableName countScan() ConsumedCapacity = ${result.getConsumedCapacity()}")

        val newCount = count + result.getCount
        Option { result.getLastEvaluatedKey } match {
          case None   => Future.successful(newCount)
          case optKey => local(newCount, optKey)
        }
      }

    local()
  }

  type RangeCondition = (String,Condition)

  /**
    * A method overloading container for [[query]].
    *
    * This class contains the overloaded implementations of [[query]].
    *
    * @tparam T
    *     the type of the object returned by the query.
    * @see [[query]]
    */
  trait QueryMagnet[T] {
    def apply(): Future[Seq[T]]
  }

  object QueryMagnet {

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
    implicit def queryRequest1
        [T]
        (queryRequest: QueryRequest)
        (implicit serializer: DynamoDBSerializer[T])
        : QueryMagnet[T] =
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
    implicit def queryRequest2
        [T]
        (tuple: /* queryRequest */ (QueryRequest,
               /* totalLimit    */  Int))
        (implicit serializer: DynamoDBSerializer[T])
        : QueryMagnet[T] =
      queryRaw(tuple._1, Some(tuple._2))

    private def queryRaw
        [T]
        (queryRequest: QueryRequest, totalLimit: Option[Int] = None)
        (implicit serializer: DynamoDBSerializer[T])
        : QueryMagnet[T] = new QueryMagnet[T] { def apply() = {
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
            logger.debug(s"table $tableName query() ConsumedCapacity = ${result.getConsumedCapacity}")

          val queryResult = result.getItems.asScala map { item =>
              serializer.fromAttributeMap(item.asScala)
          }

          builder ++= (numberLeftToFetch match {
            case Some(n) if n <= queryResult.size => queryResult.take(n)
            case _                                => queryResult
          })

          val optKey = Option { result.getLastEvaluatedKey }

          if (optKey.isEmpty) Future.successful(())
          else numberLeftToFetch match {
            case Some(n) if n <= queryResult.size => Future.successful(())
            case Some(n)                          => local(optKey, Some(n - queryResult.size))
            case None                             => local(optKey, None)
          }
        }

      local(None, totalLimit) map { _ => builder.result() }
    }}


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
    implicit def queryOnHash1
        [T, K]
        (hashValue: K)
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : QueryMagnet[T] =
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
    implicit def queryOnHash2
        [T, K]
        (tuple: /* hashValue  */ (K,
                /* totalLimit */  Int))
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : QueryMagnet[T] =
      queryOnHash(tuple._1, Some(tuple._2))

    private def queryOnHash
        [T, K]
        (hashValue: K, totalLimit: Option[Int] = None)
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : QueryMagnet[T] =
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
    implicit def queryOnHashAndRange1
        [T, K]
        (tuple: /* hashValue        */ (K,
                /* rangeCondition   */  Condition))
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : QueryMagnet[T] =
      queryOnHashAndRange(tuple._1, tuple._2, true, None)

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
    implicit def queryOnHashAndRange2
        [T, K]
        (tuple: /* hashValue        */ (K,
                /* rangeCondition   */  Condition,
                /* scanIndexForward */  Boolean))
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : QueryMagnet[T] =
      queryOnHashAndRange(tuple._1, tuple._2, tuple._3, None)

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
    implicit def queryOnHashAndRange3
        [T, K]
        (tuple: /* hashValue        */ (K,
                /* rangeCondition   */  Condition,
                /* totalLimit       */  Int))
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : QueryMagnet[T] =
      queryOnHashAndRange(tuple._1, tuple._2, true, Some(tuple._3))

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
    implicit def queryOnHashAndRange4
        [T, K]
        (tuple: /* hashValue        */ (K,
                /* rangeCondition   */  Condition,
                /* scanIndexForward */  Boolean,
                /* totalLimit       */  Int))
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : QueryMagnet[T] =
      queryOnHashAndRange(tuple._1, tuple._2, tuple._3, Some(tuple._4))

    private def queryOnHashAndRange
        [T, K]
        (hashValue:         K,
         rangeCondition:    Condition,
         scanIndexForward:  Boolean,
         totalLimit:        Option[Int])
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : QueryMagnet[T] =
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
    implicit def queryOnSecondaryIndex1
      [T, K]
      (tuple: /* indexName          */ (String,
              /* hashValue          */  K,
              /* rangeAttributeName */  String,
              /* rangeCondition     */  Condition))
      (implicit serializer: DynamoDBSerializer[T],
                ev: K => AttributeValue)
      : QueryMagnet[T] =
      queryOnSecondaryIndex(tuple._1, tuple._2, tuple._3, tuple._4, true, None)

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
    implicit def queryOnSecondaryIndex2
      [T, K]
      (tuple: /* indexName          */ (String,
              /* hashValue          */  K,
              /* rangeAttributeName */  String,
              /* rangeCondition     */  Condition,
              /* scanIndexForward   */  Boolean))
      (implicit serializer: DynamoDBSerializer[T],
                ev: K => AttributeValue)
      : QueryMagnet[T] =
      queryOnSecondaryIndex(tuple._1, tuple._2, tuple._3, tuple._4, tuple._5, None)

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
    implicit def queryOnSecondaryIndex3
      [T, K]
      (tuple: /* indexName          */ (String,
              /* hashValue          */  K,
              /* rangeAttributeName */  String,
              /* rangeCondition     */  Condition,
              /* totalLimit         */  Int))
      (implicit serializer: DynamoDBSerializer[T],
                ev: K => AttributeValue)
      : QueryMagnet[T] =
      queryOnSecondaryIndex(tuple._1, tuple._2, tuple._3, tuple._4, true, Some(tuple._5))

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
    implicit def queryOnSecondaryIndex4
      [T, K]
      (tuple: /* indexName          */ (String,
              /* hashValue          */  K,
              /* rangeAttributeName */  String,
              /* rangeCondition     */  Condition,
              /* scanIndexForward   */  Boolean,
              /* totalLimit         */  Int))
      (implicit serializer: DynamoDBSerializer[T],
                ev: K => AttributeValue)
      : QueryMagnet[T] =
      queryOnSecondaryIndex(tuple._1, tuple._2, tuple._3, tuple._4, tuple._5, Some(tuple._6))

    private def queryOnSecondaryIndex
        [T, K]
        (indexName:           String,
         hashValue:           K,
         rangeAttributeName:  String,
         rangeCondition:      Condition,
         scanIndexForward:    Boolean     = true,
         totalLimit:          Option[Int] = None)
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : QueryMagnet[T] =
      queryOnAnyIndex(indexName, serializer.hashAttributeName, hashValue, Some((rangeAttributeName, rangeCondition)), scanIndexForward, totalLimit)

    /**
     * Query a global secondary index by a hash value and optional
     * range condition, ascending or desending, with a limit.
     *
     * This query targets a named global secondary index. The index
     * being used must be named, as well as the name of
     * the hash attribute used and the target key of the index.
     * If the index contains a range then the name of
     * the range attribute used must also be given along with the
     * target range key.
     * The result will be all items with the same hash key
     * value, and range keys that match the optional range condition.
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
     * @param hashAttributeName
      *     the name of the key attribute used by the index.
     * @param hashValue
      *     the hash key value to match.
     * @param rangeCondition
      *     an optional tuple who's first member is the name (String) of the range key attribute used by the index,
      *     and who's second member is the Condition to apply to the range key.
     * @param scanIndexForward
      *     true for forwards scan, and false for reverse scan.
     * @param totalLimit
      *     the total number of results you want.
     * @param serializer
      *     an implicit object serializer.
     * @return result sequence of the query in a future.
     * @see [[query]]
     */
    implicit def queryOnGlobalSecondaryIndex
      [T, K]
      (tuple: /* indexName          */ (String,
              /* hashAttributeName  */  String,
              /* hashValue          */  K,
              /* rangeCondition     */  Option[RangeCondition],
              /* scanIndexForward   */  Boolean,
              /* totalLimit         */  Int))
      (implicit serializer: DynamoDBSerializer[T],
       ev: K => AttributeValue)
      : QueryMagnet[T] =
        queryOnAnyIndex(tuple._1, tuple._2, tuple._3, tuple._4, tuple._5, Some(tuple._6))

    private def queryOnAnyIndex
      [T, K]
      (indexName:           String,
       hashAttributeName:   String,
       hashValue:           K,
       rangeCondition:      Option[RangeCondition],
       scanIndexForward:    Boolean     = true,
       totalLimit:          Option[Int] = None)
      (implicit serializer: DynamoDBSerializer[T],
                ev: K => AttributeValue)
      : QueryMagnet[T] = {

        val keyConditions = {
          val b = Map.newBuilder[String, Condition]
          b += (hashAttributeName -> QueryCondition.equalTo(hashValue))
          rangeCondition.foreach(b += _)
          b.result()
        }

        queryRaw(
          new QueryRequest()
            .withIndexName(indexName)
            .withKeyConditions( keyConditions.asJava )
            .withSelect(Select.ALL_ATTRIBUTES)
            .withScanIndexForward(scanIndexForward),
          totalLimit
        )
    }
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
    * @see [[QueryMagnet]]
    * @see [[queryOnce]]
    * @see [[countQuery]]
    */
  def query[T](magnet: QueryMagnet[T]): Future[Seq[T]] = magnet()


  trait QueryOnceMagnet[T] {
    def apply(): Future[Seq[T]]
  }

  object QueryOnceMagnet {

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
    implicit def queryOnceWithQueryRequest
        [T]
        (queryRequest: QueryRequest)
        (implicit serializer: DynamoDBSerializer[T])
        : QueryOnceMagnet[T] = new QueryOnceMagnet[T] { def apply(): Future[Seq[T]] = {
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
          logger.debug(s"table $tableName queryOnce() ConsumedCapacity = ${result.getConsumedCapacity()}")

        result.getItems.asScala.view map { item =>
          serializer.fromAttributeMap(item.asScala)
        }
      }
    }}

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
    def queryOnceWithHashValue
        [T, K]
        (hashValue: K,
         limit:     Int)
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : QueryOnceMagnet[T] = {
      val request = mkHashKeyQuery(hashValue)
      if (limit > 0) request.setLimit(limit)
      queryOnceWithQueryRequest(request)
    }
    implicit def queryOnceWithHashValue1
        [T, K]
        (tuple: /* hashValue */ (K,
                /* limit     */  Int))
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : QueryOnceMagnet[T] =
      queryOnceWithHashValue(tuple._1, tuple._2)
    implicit def queryOnceWithHashValue2
        [T, K]
        (hashValue: K)
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : QueryOnceMagnet[T] =
      queryOnceWithHashValue(hashValue, 0)


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
    def queryOnceWithHashValueRangeCondition
        [T, K]
        (hashValue:        K,
         rangeCondition:   Condition,
         scanIndexForward: Boolean,
         limit:            Int)
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : QueryOnceMagnet[T] = {
      val request =
        mkHashAndRangeKeyQuery(hashValue, rangeCondition)
        .withScanIndexForward(scanIndexForward)
      if (limit > 0) request.setLimit(limit)
      queryOnceWithQueryRequest(request)
    }
    implicit def queryOnceWithHashValueRangeCondition1
        [T, K]
        (tuple: /* hashValue        */ (K,
                /* rangeCondition   */  Condition,
                /* scanIndexForward */  Boolean,
                /* limit            */  Int))
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : QueryOnceMagnet[T] =
      queryOnceWithHashValueRangeCondition(tuple._1, tuple._2, tuple._3, tuple._4)
    implicit def queryOnceWithHashValueRangeCondition2
        [T, K]
        (tuple: /* hashValue        */ (K,
                /* rangeCondition   */  Condition,
                /* scanIndexForward */  Boolean))
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : QueryOnceMagnet[T] =
      queryOnceWithHashValueRangeCondition(tuple._1, tuple._2, tuple._3, 0)
    implicit def queryOnceWithHashValueRangeCondition3
        [T, K]
        (tuple: /* hashValue        */ (K,
                /* rangeCondition   */  Condition,
                /* limit            */  Int))
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : QueryOnceMagnet[T] =
      queryOnceWithHashValueRangeCondition(tuple._1, tuple._2, true, tuple._3)
    implicit def queryOnceWithHashValueRangeCondition4
        [T, K]
        (tuple: /* hashValue        */ (K,
                /* rangeCondition   */  Condition))
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : QueryOnceMagnet[T] =
      queryOnceWithHashValueRangeCondition(tuple._1, tuple._2, true, 0)

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
    def queryOnceSecondaryIndex
        [T, K]
        (indexName:          String,
         hashValue:          K,
         rangeAttributeName: String,
         rangeCondition:     Condition,
         scanIndexForward:   Boolean,
         limit:              Int)
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : QueryOnceMagnet[T] =
      queryOnceOnAnyIndex(indexName, serializer.hashAttributeName, hashValue, Some((rangeAttributeName, rangeCondition)), scanIndexForward, limit)

    /**
     * Query a global secondary index by a hash value and optional
     * range condition, ascending or desending, with a limit.
     *
     * This query targets a named global secondary index. The index
     * being used must be named, as well as the name of
     * the hash attribute used and the target key of the index.
     * If the index contains a range then the name of
     * the range attribute used must also be given along with the
     * target range key.
     * The result will be all items with the same hash key
     * value, and range keys that match the optional range condition.
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
     * @param hashAttributeName
      *     the name of the key attribute used by the index.
     * @param hashValue
      *     the hash key value to match.
     * @param rangeCondition
      *     an optional tuple who's first member is the name (String) of the range key attribute used by the index,
     *     and who's second member is the Condition to apply to the range key.
     * @param scanIndexForward
      *     true for forwards scan, and false for reverse scan.
     * @param totalLimit
      *     the total number of results you want.
     * @param serializer
      *     an implicit object serializer.
     * @return result sequence of the query in a future.
     * @see [[query]]
     */
    implicit def queryOnceOnGlobalSecondaryIndex
    [T, K]
    (tuple: /* indexName          */ (String,
      /* hashAttributeName  */  String,
      /* hashValue          */  K,
      /* rangeCondition     */  Option[RangeCondition],
      /* scanIndexForward   */  Boolean,
      /* totalLimit         */  Int))
    (implicit serializer: DynamoDBSerializer[T],
     ev: K => AttributeValue)
    : QueryOnceMagnet[T] =
      queryOnceOnAnyIndex(tuple._1, tuple._2, tuple._3, tuple._4, tuple._5, tuple._6)

    private def queryOnceOnAnyIndex
        [T, K]
        (indexName:           String,
         hashAttributeName:   String,
         hashValue:           K,
         rangeCondition:      Option[RangeCondition],
         scanIndexForward:    Boolean     = true,
         totalLimit:          Int)
        (implicit serializer: DynamoDBSerializer[T],
         ev: K => AttributeValue)
        : QueryOnceMagnet[T] = {
      val keyConditions = {
        val b = Map.newBuilder[String, Condition]
        b += (hashAttributeName -> QueryCondition.equalTo(hashValue))
        rangeCondition.foreach(b += _)
        b.result()
      }

      val request =
        new QueryRequest()
          .withIndexName(indexName)
          .withKeyConditions( keyConditions.asJava )
          .withSelect(Select.ALL_ATTRIBUTES)
          .withScanIndexForward(scanIndexForward)

      if (totalLimit > 0) request.setLimit(totalLimit)
      queryOnceWithQueryRequest(request)
    }

    implicit def queryOnceSecondaryIndex1
        [T, K]
        (tuple: /* indexName          */ (String,
                /* hashValue          */  K,
                /* rangeAttributeName */  String,
                /* rangeCondition     */  Condition,
                /* scanIndexForward   */  Boolean,
                /* limit              */  Int))
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : QueryOnceMagnet[T] =
      queryOnceSecondaryIndex(tuple._1, tuple._2, tuple._3, tuple._4, tuple._5, tuple._6)
    implicit def queryOnceSecondaryIndex2
        [T, K]
        (tuple: /* indexName          */ (String,
                /* hashValue          */  K,
                /* rangeAttributeName */  String,
                /* rangeCondition     */  Condition,
                /* scanIndexForward   */  Boolean))
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : QueryOnceMagnet[T] =
      queryOnceSecondaryIndex(tuple._1, tuple._2, tuple._3, tuple._4, tuple._5, 0)
    implicit def queryOnceSecondaryIndex3
        [T, K]
        (tuple: /* indexName          */ (String,
                /* hashValue          */  K,
                /* rangeAttributeName */  String,
                /* rangeCondition     */  Condition,
                /* limit              */  Int))
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : QueryOnceMagnet[T] =
      queryOnceSecondaryIndex(tuple._1, tuple._2, tuple._3, tuple._4, true, tuple._5)
    implicit def queryOnceSecondaryIndex4
        [T, K]
        (tuple: /* indexName          */ (String,
                /* hashValue          */  K,
                /* rangeAttributeName */  String,
                /* rangeCondition     */  Condition))
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : QueryOnceMagnet[T] =
      queryOnceSecondaryIndex(tuple._1, tuple._2, tuple._3, tuple._4, true, 0)
  }

  /**
    * Query a table.
    *
    * This method will issue one query request, stopping either
    * at the supplied limit or at the response size limit.
    *
    * @tparam T
    *     the type of the object returned by the query.
    * @see [[QueryOnceMagnet]]
    * @see [[query]]
    * @see [[countQuery]]
    */
  def queryOnce[T](magnet: QueryOnceMagnet[T]): Future[Seq[T]] = magnet()


  /**
    * A method overloading container for [[countQuery]].
    *
    * This class contains the overloaded implementations of [[countQuery]].
    *
    * @tparam T
    *     the type of object queried.
    * @see [[countQuery]]
    */
  trait CountQueryMagnet[T] {
    def apply(): Future[Long]
  }

  object CountQueryMagnet {

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
    implicit def countQueryRequest
        [T]
        (queryRequest: QueryRequest)
        (implicit serializer: DynamoDBSerializer[T])
        : CountQueryMagnet[T] = new CountQueryMagnet[T] { def apply() = {
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
            logger.debug(s"table $tableName countQuery() ConsumedCapacity = ${result.getConsumedCapacity()}")

          val newCount = count + result.getCount

          Option { result.getLastEvaluatedKey } match {
            case None   => Future.successful(newCount)
            case optKey => local(newCount, optKey)
          }
        }

      local()
    }}

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
    implicit def countQueryHashValue
        [T, K]
        (hashValue: K)
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : CountQueryMagnet[T] =
      countQueryRequest(mkHashKeyQuery(hashValue))

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
    implicit def countQueryHashValueAndRangeCondition
        [T, K]
        (tuple: /* hashValue      */ (K,
                /* rangeCondition */  Condition))
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : CountQueryMagnet[T] =
      countQueryRequest(mkHashAndRangeKeyQuery(tuple._1, tuple._2))

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
    implicit def countQuerySecondaryIndex
        [T, K]
        (tuple: /* indexName          */ (String,
                /* hashValue          */  K,
                /* rangeAttributeName */  String,
                /* rangeCondition     */  Condition))
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : CountQueryMagnet[T] =
      countQueryRequest(
        new QueryRequest()
          .withIndexName(tuple._1)
          .withKeyConditions(
            Map(
              serializer.hashAttributeName -> QueryCondition.equalTo(tuple._2),
              tuple._3                     -> tuple._4
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
    * @see [[CountQueryMagnet]]
    * @see [[query]]
    * @see [[queryOnce]]
    */
  def countQuery[T](magnet: CountQueryMagnet[T]): Future[Long] = magnet()



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
  private def zipKeySeqs[T, K1, K2]
                        (hashKeys:  Seq[K1], rangeKeys: Seq[K2] = Seq.empty)
                        (implicit serializer: DynamoDBSerializer[T],
                                  ev1: K1 => AttributeValue,
                                  ev2: K2 => AttributeValue)
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
  trait BatchLoadByKeysMagnet[T] {
    def apply(): Future[Seq[T]]
  }

  object BatchLoadByKeysMagnet {

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
    implicit def batchLoadByHashKeys
        [T, K]
        (hashKeys:  Seq[K])
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : BatchLoadByKeysMagnet[T] =
      batchLoadByHashAndRangeKeys(hashKeys -> Seq.empty[String])
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
    implicit def batchLoadByHashAndRangeKeys
        [T, K1, K2]
        (tuple: /* hashKeys  */ (Seq[K1],
                /* rangeKeys */  Seq[K2]))
        (implicit serializer: DynamoDBSerializer[T],
                  ev1: K1 => AttributeValue,
                  ev2: K2 => AttributeValue)
        : BatchLoadByKeysMagnet[T] = new BatchLoadByKeysMagnet[T] { def apply() = {
      val keys: Seq[DynamoDBKey] = zipKeySeqs(tuple._1, tuple._2)
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
            logger.debug(s"table $tableName batchLoadByKeys() ConsumedCapacity = ${result.getConsumedCapacity()}")

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
    }}
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
    * @see [[BatchLoadByKeysMagnet]]
    */
  def batchLoadByKeys[T](magnet: BatchLoadByKeysMagnet[T]): Future[Seq[T]] = magnet()



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
          logger.debug(s"table $tableName batchDump() ConsumedCapacity = ${result.getConsumedCapacity()}")

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
          logger.debug(s"table $tableName batchDelete() ConsumedCapacity = ${result.getConsumedCapacity()}")

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
  trait BatchDeleteByKeysMagnet[T] {
    def apply(): Future[Unit]
  }

  object BatchDeleteByKeysMagnet {

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
    implicit def batchDeleteByHashKeys
        [T, K]
        (hashKeys: Seq[K])
        (implicit serializer: DynamoDBSerializer[T],
                  ev: K => AttributeValue)
        : BatchDeleteByKeysMagnet[T] =
      batchDeleteByHashAndRangeKeys(hashKeys -> Seq.empty[String])
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
    implicit def batchDeleteByHashAndRangeKeys
        [T, K1, K2]
        (tuple: /* hashKeys  */ (Seq[K1],
                /* rangeKeys */  Seq[K2]))
        (implicit serializer: DynamoDBSerializer[T],
                  ev1: K1 => AttributeValue,
                  ev2: K2 => AttributeValue)
        : BatchDeleteByKeysMagnet[T] = new BatchDeleteByKeysMagnet[T] { def apply() = {
      val keys: Seq[DynamoDBKey] = zipKeySeqs(tuple._1, tuple._2)

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
            logger.debug(s"table $tableName batchDeleteByKeys() ConsumedCapacity = ${result.getConsumedCapacity()}")

          checkRetryBatchWrite(result) flatMap { _ =>
            if (keysPair._2.isEmpty)
              Future.successful(())
            else
              local(keysPair._2.splitAt(25))
          }
        }
      }

      local(keys.splitAt(25))
    }}
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
    * @see [[BatchDeleteByKeysMagnet]]
    */
  def batchDeleteByKeys[T](magnet: BatchDeleteByKeysMagnet[T]): Future[Unit] = magnet()

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
