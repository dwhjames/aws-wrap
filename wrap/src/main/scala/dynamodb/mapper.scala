package aws.wrap
package dynamodb

import scala.concurrent.{Future, ExecutionContext}
import scala.collection.JavaConverters._
import scala.collection.mutable

import java.util.{Map => JMap}

import com.amazonaws.services.dynamodbv2.model._

trait DynamoDBSerializer[T] {
  def tableName: String
  def hashAttributeName: String
  def rangeAttributeName: Option[String] = None

  def fromAttributeMap(item: mutable.Map[String, AttributeValue]): T

  def toAttributeMap(obj: T): Map[String, AttributeValue]

  /*
   * A helper for implementing toAttributeMap
   *
   * override def toAttributeMap(obj: Foo): Map[String, AttributeValue] =
   *   Map(
   *     mkAtrribute("company", obj.company),
   *     ...
   *   )
   */
  protected def mkAttribute[K](name: String, value: K)(implicit conv: K => AttributeValue): (String, AttributeValue) =
    (name, conv(value))

  protected def mkAttribute[K](pair: (String, K))(implicit conv: K => AttributeValue): (String, AttributeValue) =
    (pair._1, conv(pair._2))

  def primaryKeyOf(obj: T): Map[String, AttributeValue] = {
    val attributes = toAttributeMap(obj)
    if (rangeAttributeName.isEmpty)
      Map(hashAttributeName -> attributes(hashAttributeName))
    else
      Map(
        hashAttributeName      -> attributes(hashAttributeName),
        rangeAttributeName.get -> attributes(rangeAttributeName.get)
      )
  }

  def makeKey[K](hashKey: K)(implicit conv: K => AttributeValue): Map[String, AttributeValue] =
    Map(hashAttributeName -> conv(hashKey))

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

trait AmazonDynamoDBScalaMapperConfig {
  def transformTableName(tableName: String): String
}

object AmazonDynamoDBScalaMapperConfig {
  object Default extends AmazonDynamoDBScalaMapperConfig {
    override def transformTableName(tableName: String) = tableName
  }
}


trait AmazonDynamoDBScalaMapper {

  val client: AmazonDynamoDBScalaClient

  protected implicit val execCtx: ExecutionContext

  protected val config: AmazonDynamoDBScalaMapperConfig =
    AmazonDynamoDBScalaMapperConfig.Default

  protected def tableName[T](implicit serializer: DynamoDBSerializer[T]): String =
    config.transformTableName(serializer.tableName)

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
      ) map { result =>
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
      ) map { result =>
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
      tableName = tableName,
      key       = serializer.primaryKeyOf(obj)
    ) map { _ => () }

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
      tableName = tableName,
      item      = serializer.toAttributeMap(obj)
    ) map { _ => () }

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
        tableName = tableName,
        key       = serializer.makeKey(hashKey)
      ) map { result =>
        serializer.fromAttributeMap(result.getItem.asScala)
      }

    def apply[K1 <% AttributeValue, K2 <% AttributeValue]
             (hashKey: K1, rangeKey: K2)
             (implicit serializer: DynamoDBSerializer[T])
             : Future[T] =
      client.getItem(
        tableName = tableName,
        key       = serializer.makeKey(hashKey, rangeKey)
      ) map { result =>
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
    val builder = Seq.newBuilder[T]

    def local(lastKey: Option[JMap[String, AttributeValue]] = None): Future[Unit] =
      client.scan(
        scanRequest.withExclusiveStartKey(lastKey.orNull)
      ) flatMap { result =>
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

    def local(count: Long = 0L, lastKey: Option[JMap[String, AttributeValue]] = None): Future[Long] =
      client.scan(
        scanRequest.withExclusiveStartKey(lastKey.orNull)
      ) flatMap { result =>
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
      val builder = Seq.newBuilder[T]

      def local(lastKey: Option[JMap[String, AttributeValue]] = None): Future[Unit] =
        client.query(
          queryRequest.withExclusiveStartKey(lastKey.orNull)
        ) flatMap { result =>
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

    def local(count: Long = 0L, lastKey: Option[JMap[String, AttributeValue]] = None): Future[Long] =
      client.query(
        queryRequest.withExclusiveStartKey(lastKey.orNull)
      ) flatMap { result =>
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
            Map(
              tableName ->
                new KeysAndAttributes()
                .withKeys(
                  keys._1.asJavaCollection
                )
            )
          ) flatMap { result =>
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
        .withRequestItems(retryItems)
      ) map { result =>
        if (!result.getUnprocessedItems.isEmpty)
          throw new Exception("AmazonDynamoDBScalaMapper: batch write retry failed")
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
    */
  def batchDump[T](objs: Seq[T])(implicit serializer: DynamoDBSerializer[T]): Future[Unit] = {
    def local(objsP: (Seq[T], Seq[T])): Future[Unit] =
      client.batchWriteItem(
        new BatchWriteItemRequest()
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
