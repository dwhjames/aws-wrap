package aws.wrap
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
    * [[DynamoDBSerializer.toAttributeMap]].
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
    *     An value that is convertable to an [[AttributeValue]].
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
    *     An value that is convertable to an [[AttributeValue]].
    * @param rangeKey
    *     An value that is convertable to an [[AttributeValue]].
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

object AmazonDynamoDBScalaMapperConfig {

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
    * Delete a DynamoDB item by a hash key and range key.
    *
    * The object that was deleted is returned.
    *
    * @param hashKey
    *     A string, number, or byte array that is the hash key value of the
    *     item to be deleted
    * @param rangeKey
    *     A string, number, or byte array that is the range key value of the
    *     item to be deleted
    * @return object that was deleted in a future
    */
  def deleteByKey[T] = new {
    def apply[K <% AttributeValue]
             (hashKey: K)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[T] =
      client.deleteItem(
        new DeleteItemRequest()
        .withTableName(tableName)
        .withKey(serializer.makeKey(hashKey).asJava)
        .withReturnValues(ReturnValue.ALL_OLD)
        .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      ) map { result =>
        logger.debug(s"deleteByKey() ConsumedCapacity = ${result.getConsumedCapacity()}")
        serializer.fromAttributeMap(result.getAttributes.asScala)
      }

    def apply[K1 <% AttributeValue, K2 <% AttributeValue]
             (hashKey: K1, rangeKey: K2)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[T] =
      client.deleteItem(
        new DeleteItemRequest()
        .withTableName(tableName)
        .withKey(serializer.makeKey(hashKey, rangeKey).asJava)
        .withReturnValues(ReturnValue.ALL_OLD)
        .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      ) map { result =>
        logger.debug(s"deleteByKey() ConsumedCapacity = ${result.getConsumedCapacity()}")
        serializer.fromAttributeMap(result.getAttributes.asScala)
      }
  }

  /**
    * Delete the DynamoDB item that corresponds to the given object
    *
    * @param obj
    *     The object to delete
    */
  def delete[T](
    obj: T
  )(implicit serializer: DynamoDBSerializer[T]): Future[Unit] =
    client.deleteItem(
      new DeleteItemRequest()
      .withTableName(tableName)
      .withKey(serializer.primaryKeyOf(obj).asJava)
      .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
    ) map { result =>
      logger.debug(s"delete() ConsumedCapacity = ${result.getConsumedCapacity()}")
      ()
    }

  /**
    * Dumps an object into DynamoDB
    *
    * If the object is new, then this creates the item in DynamoDB,
    * otherwise it overwrites the exisiting item.
    *
    * @param obj
    *     the object to put
    */
  def dump[T](
    obj: T
  )(implicit serializer: DynamoDBSerializer[T]): Future[Unit] =
    client.putItem(
      new PutItemRequest()
      .withTableName(tableName)
      .withItem(serializer.toAttributeMap(obj).asJava)
      .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
    ) map { result =>
      logger.debug(s"dump() ConsumedCapacity = ${result.getConsumedCapacity()}")
      ()
    }

  /**
    * Load an object by its hash key and range key
    *
    * @param hashKey
    *     the hash key of the object to retrieve
    * @param rangeKey
    *     the range key of the object to retrieve
    * @return the retreived object in a future
    */
  def loadByKey[T] = new {
    def apply[K <% AttributeValue]
             (hashKey: K)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[T] =
      client.getItem(
        new GetItemRequest()
        .withTableName(tableName)
        .withKey(serializer.makeKey(hashKey).asJava)
        .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
        .withConsistentRead(config.consistentReads)
      ) map { result =>
        logger.debug(s"loadByKey() ConsumedCapacity = ${result.getConsumedCapacity()}")
        serializer.fromAttributeMap(result.getItem.asScala)
      }

    def apply[K1 <% AttributeValue, K2 <% AttributeValue]
             (hashKey: K1, rangeKey: K2)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[T] =
      client.getItem(
        new GetItemRequest()
        .withTableName(tableName)
        .withKey(serializer.makeKey(hashKey, rangeKey).asJava)
        .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
        .withConsistentRead(config.consistentReads)
      ) map { result =>
        logger.debug(s"loadByKey() ConsumedCapacity = ${result.getConsumedCapacity()}")
        serializer.fromAttributeMap(result.getItem.asScala)
      }
  }

  /**
    * Scan a table.
    *
    * This method will internally make repeated scan calls
    * until the full result of the scan has been retrieved.
    *
    * @param scanFilter
    *     the optional filter conditions for the scan
    * @return sequence of scanned objects in a future
    */
  def scan[T](
    scanFilter: Map[String, Condition] = Map.empty
  )(implicit serializer: DynamoDBSerializer[T]): Future[Seq[T]] = {
    val scanRequest =
      new ScanRequest()
      .withTableName(tableName)
      .withScanFilter(scanFilter.asJava)
      .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
    val builder = Seq.newBuilder[T]

    def local(lastKey: Option[JMap[String, AttributeValue]] = None): Future[Unit] =
      client.scan(
        scanRequest.withExclusiveStartKey(lastKey.orNull)
      ) flatMap { result =>
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
    * Scan a table and return a count.
    *
    * This method will internally make repeated scan calls
    * until the full result of the scan has been retrieved.
    *
    * @param scanFilter
    *     the optional filter conditions for the scan
    * @return the total number of scanned items in a future
    */
  def countScan[T](
    scanFilter: Map[String, Condition] = Map.empty
  )(implicit serializer: DynamoDBSerializer[T]): Future[Long] = {
    val scanRequest =
      new ScanRequest()
      .withTableName(tableName)
      .withScanFilter(scanFilter.asJava)
      .withSelect(Select.COUNT)
      .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

    def local(count: Long = 0L, lastKey: Option[JMap[String, AttributeValue]] = None): Future[Long] =
      client.scan(
        scanRequest.withExclusiveStartKey(lastKey.orNull)
      ) flatMap { result =>
        logger.debug(s"countScan() ConsumedCapacity = ${result.getConsumedCapacity()}")
        Option { result.getLastEvaluatedKey } match {
          case None   => Future.successful(count)
          case optKey => local(count + result.getCount, optKey)
        }
      }

    local()
  }

  /**
    * Query a table
    *
    * This method will internally make repeated query calls
    * until the full result of the query has been retrieved.
    *
    * @param queryRequest
    *     the query parameters
    * @return sequence of queries objects in a future
    */
  def query[T] = new {
    def apply(queryRequest: QueryRequest)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Seq[T]] = {
      val request =
        queryRequest
        .withTableName(tableName)
        .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
        .withConsistentRead(config.consistentReads)
      val builder = Seq.newBuilder[T]

      def local(lastKey: Option[JMap[String, AttributeValue]] = None): Future[Unit] =
        client.query(
          queryRequest.withExclusiveStartKey(lastKey.orNull)
        ) flatMap { result =>
          logger.debug(s"query() ConsumedCapacity = ${result.getConsumedCapacity()}")
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

    def apply[K <% AttributeValue]
             (hashValue: K)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Seq[T]] =
      apply(mkHashKeyQuery(hashValue))

    def apply[K <% AttributeValue]
             (hashValue: K, rangeCondition: Condition)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Seq[T]] =
      apply(mkHashAndRangeKeyQuery(hashValue, rangeCondition))
  }

  /**
    * Query a table and return a count.
    *
    * This method will internally make repeated query calls
    * until the full result of the query has been retrieved.
    *
    * @param keyConditions
    *     the query conditions on the keys
    * @return the total number of queried items in a future
    */
  def countQuery[T](
    keyConditions: Map[String, Condition]
  )(implicit serializer: DynamoDBSerializer[T]): Future[Long] = {
    val queryRequest =
      new QueryRequest()
      .withTableName(tableName)
      .withKeyConditions(keyConditions.asJava)
      .withSelect(Select.COUNT)
      .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      .withConsistentRead(config.consistentReads)

    def local(count: Long = 0L, lastKey: Option[JMap[String, AttributeValue]] = None): Future[Long] =
      client.query(
        queryRequest.withExclusiveStartKey(lastKey.orNull)
      ) flatMap { result =>
        logger.debug(s"countQuery() ConsumedCapacity = ${result.getConsumedCapacity()}")
        Option { result.getLastEvaluatedKey } match {
          case None   => Future.successful(count)
          case optKey => local(count + result.getCount, optKey)
        }
      }

    local()
  }

  /**
    * Load a sequence of objects by a sequence of keys.
    *
    * This method will internally make repeated batchGetItem
    * calls, with up to 25 keys at a time, until all of the
    * given keys have been requested.
    *
    * @param hashKeys
    *     the hash keys of the objects to retrieve
    * @param rangeKeys
    *     the range keys of the objects to retrieve
    * @return sequence of retrieved objects in a future
    */
  def batchLoadByKeys[T] = new {
    def apply[K1 <% AttributeValue, K2 <% AttributeValue]
             (hashKeys:  Seq[K1], rangeKeys: Seq[K2] = Seq.empty)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[Seq[T]] =
      if (hashKeys.isEmpty) {
        throw new IllegalArgumentException("AmazonDynamoDBScalaMapper.batchLoad: no hash keys given")
      } else if (!rangeKeys.isEmpty && (hashKeys.length != rangeKeys.length)) {
        throw new IllegalArgumentException("AmazonDynamoDBScalaMapper.batchLoad: the number of hash and range keys don't match")
      } else {
        val keys: Seq[JMap[String, AttributeValue]] = if (rangeKeys.isEmpty) {
          hashKeys.map { hashKey =>
            serializer.makeKey(hashKey).asJava
          }
        } else {
          (hashKeys, rangeKeys).zipped.map { case (hashKey, rangeKey) =>
            serializer.makeKey(hashKey, rangeKey).asJava
          }
        }
        val builder = Seq.newBuilder[T]

        def local(keys: (Seq[JMap[String, AttributeValue]], Seq[JMap[String, AttributeValue]])): Future[Unit] =
          client.batchGetItem(
            new BatchGetItemRequest()
            .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
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
          ) flatMap { result =>
            logger.debug(s"batchLoadByKeys() ConsumedCapacity = ${result.getConsumedCapacity()}")
            builder ++= result.getResponses.get(tableName).asScala.view map { item =>
              serializer.fromAttributeMap(item.asScala)
            }
            if (keys._2.isEmpty)
              Future.successful(())
            else
              local(keys._2.splitAt(100))
          }

        local(keys.splitAt(100)) map { _ => builder.result }
      }
  }

  private def checkRetryBatchWrite(lastResult: BatchWriteItemResult): Future[Unit] = {
    val retryItems = lastResult.getUnprocessedItems
    if (retryItems.isEmpty)
      Future.successful(())
    else
      client.batchWriteItem(
        new BatchWriteItemRequest()
        .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
        .withRequestItems(retryItems)
      ) map { result =>
        logger.debug(s"checkRetryBatchWrite() ConsumedCapacity = ${result.getConsumedCapacity()}")
        if (!result.getUnprocessedItems.isEmpty)
          throw new BatchDumpException("AmazonDynamoDBScalaMapper: batch write retry failed", result.getUnprocessedItems)
      }
  }

  /**
    * Dump a sequence of objects into DynamoDB
    *
    * This method will internally make repeated batchWriteItem
    * calls, with up to 25 objects at a time, until all the input
    * objects have been written. If any objects fail to be written,
    * they will be retried once, and an exception will be thrown
    * on their second failure.
    *
    * Objects that are new will create new items in DynamoDB,
    * otherwise they will overwrite exisiting items.
    *
    * @param objs
    *     the sequence of objects to write to DynamoDB
    * @throws BatchDumpException if a write to DynamoDB fails twice
    */
  def batchDump[T](objs: Seq[T])(implicit serializer: DynamoDBSerializer[T]): Future[Unit] = {
    def local(objsP: (Seq[T], Seq[T])): Future[Unit] =
      client.batchWriteItem(
        new BatchWriteItemRequest()
        .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
        .withRequestItems(
          Map(
            tableName -> objsP._1.view.map { obj =>
              new WriteRequest()
              .withPutRequest(
                new PutRequest()
                .withItem(serializer.toAttributeMap(obj).asJava)
              )
            } .asJava
          ).asJava
        )
      ) flatMap { result =>
        logger.debug(s"batchDump() ConsumedCapacity = ${result.getConsumedCapacity()}")
        checkRetryBatchWrite(result) flatMap { _ =>
          if (objsP._2.isEmpty)
            Future.successful(())
          else
            local(objsP._2.splitAt(25))
        }
      }

    local(objs.splitAt(25))
  }

}
