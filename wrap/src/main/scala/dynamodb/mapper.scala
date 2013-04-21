
package aws.wrap
package dynamodb

import scala.concurrent.{Future, ExecutionContext}
import scala.collection.JavaConverters._
import scala.collection.mutable

import java.util.{Map => JMap}

import com.amazonaws.services.dynamodbv2.model._

trait DynamoDBObject[T] {

  val companion: DynamoDBObjectCompanion[T]

  def toAttributeMap: Map[String, AttributeValue]

  def primaryKey: Map[String, AttributeValue] = {
    val attributes = toAttributeMap
    if (companion.rangeAttributeName.isEmpty)
      Map(
        companion.hashAttributeName -> attributes(companion.hashAttributeName)
      )
    else
      Map(
        companion.hashAttributeName      -> attributes(companion.hashAttributeName),
        companion.rangeAttributeName.get -> attributes(companion.rangeAttributeName.get)
      )
  }

}

trait DynamoDBObjectCompanion[T] {

  val tableName: String

  val hashAttributeName: String

  val rangeAttributeName: Option[String] = None

  def fromAttributeMap(item: mutable.Map[String, AttributeValue]): T

  def makeKey(hashKey: Any): Map[String, AttributeValue] =
    Map(hashAttributeName -> any2AttributeValue(hashKey))

  def makeKey(hashKey: Any, rangeKey: Any): Map[String, AttributeValue] =
    Map(
      hashAttributeName -> any2AttributeValue(hashKey),
      (rangeAttributeName getOrElse {
        throw new UnsupportedOperationException(s"DynamoDBObjectCompanion.makeKey: table $tableName does not have a range key")
       } ) -> any2AttributeValue(rangeKey)
    )
  
}

trait AmazonDynamoDBScalaMapper {

  val client: AmazonDynamoDBScalaClient

  protected implicit val execCtx: ExecutionContext

  def delete[T](
    companion: DynamoDBObjectCompanion[T],
    hashKey:   Any
  ): Future[T] =
    client.deleteItem(
      new DeleteItemRequest()
      .withTableName(companion.tableName)
      .withKey(companion.makeKey(hashKey).asJava)
      .withReturnValues(ReturnValue.ALL_OLD)
    ) map { result =>
      companion.fromAttributeMap(result.getAttributes.asScala)
    }

  def delete[T](
    companion: DynamoDBObjectCompanion[T],
    hashKey:   Any,
    rangeKey:  Any
  ): Future[T] =
    client.deleteItem(
      new DeleteItemRequest()
      .withTableName(companion.tableName)
      .withKey(companion.makeKey(hashKey, rangeKey).asJava)
      .withReturnValues(ReturnValue.ALL_OLD)
    ) map { result =>
      companion.fromAttributeMap(result.getAttributes.asScala)
    }

  def delete[T](
    obj: DynamoDBObject[T]
  ): Future[Unit] =
    client.deleteItem(
      tableName = obj.companion.tableName,
      key       = obj.primaryKey
    ) map { _ => () }

  def dump[T](
    obj: DynamoDBObject[T]
  ): Future[Unit] =
    client.putItem(
      tableName = obj.companion.tableName,
      item      = obj.toAttributeMap
    ) map { _ => () }

  def load[T](
    companion: DynamoDBObjectCompanion[T],
    hashKey:   Any
  ): Future[T] =
    client.getItem(
      tableName = companion.tableName,
      key       = companion.makeKey(hashKey)
    ) map { result =>
      companion.fromAttributeMap(result.getItem.asScala)
    }

  def load[T](
    companion: DynamoDBObjectCompanion[T],
    hashKey:   Any,
    rangeKey:  Any
  ): Future[T] =
    client.getItem(
      tableName = companion.tableName,
      key       = companion.makeKey(hashKey, rangeKey)
    ) map { result =>
      companion.fromAttributeMap(result.getItem.asScala)
    }

  def scan[T](
    companion:  DynamoDBObjectCompanion[T],
    scanFilter: Map[String, Condition] = Map.empty
  ): Future[Seq[T]] = {
    val jScanFilter = scanFilter.asJava
    val builder = Seq.newBuilder[T]

    def local(lastKey: Option[JMap[String, AttributeValue]]): Future[Unit] =
      client.scan(
        new ScanRequest()
        .withTableName(companion.tableName)
        .withScanFilter(jScanFilter)
        .withExclusiveStartKey(lastKey.orNull)
      ) flatMap { result =>
        builder ++= result.getItems.asScala.view map { item =>
          companion.fromAttributeMap(item.asScala)
        }
        Option { result.getLastEvaluatedKey } match {
          case None   => Future.successful(())
          case optKey => local(optKey)
        }
      }

    local(None) map { _ => builder.result }
  }

  def query[T](
    companion: DynamoDBObjectCompanion[T],
    keyConditions: Map[String, Condition]
  ): Future[Seq[T]] = {
    val jKeyConditions = keyConditions.asJava
    val builder = Seq.newBuilder[T]

    def local(lastKey: Option[JMap[String, AttributeValue]]): Future[Unit] =
      client.query(
        new QueryRequest()
        .withTableName(companion.tableName)
        .withKeyConditions(jKeyConditions)
        .withExclusiveStartKey(lastKey.orNull)
      ) flatMap { result =>
        builder ++= result.getItems.asScala.view map { item =>
          companion.fromAttributeMap(item.asScala)
        }
        Option { result.getLastEvaluatedKey } match {
          case None   => Future.successful(())
          case optKey => local(optKey)
        }
      }

    local(None) map { _ => builder.result }
  }


  def batchLoad[T](
    companion: DynamoDBObjectCompanion[T],
    hashKeys:  Seq[Any],
    rangeKeys: Seq[Any] = Seq.empty
  ): Future[Seq[T]] =
    if (hashKeys.isEmpty) {
      throw new IllegalArgumentException("AmazonDynamoDBScalaMapper.batchLoad: no hash keys given")
    } else if (!rangeKeys.isEmpty && (hashKeys.length != rangeKeys.length)) {
      throw new IllegalArgumentException("AmazonDynamoDBScalaMapper.batchLoad: the number of hash and range keys don't match")
    } else {
      val keys: Seq[JMap[String, AttributeValue]] = if (rangeKeys.isEmpty) {
        hashKeys.map { hashKey =>
          companion.makeKey(hashKey).asJava
        }
      } else {
        (hashKeys, rangeKeys).zipped.map { case (hashKey, rangeKey) =>
          companion.makeKey(hashKey, rangeKey).asJava
        }
      }
      val builder = Seq.newBuilder[T]

      def local(keys: (Seq[JMap[String, AttributeValue]], Seq[JMap[String, AttributeValue]])): Future[Unit] =
        client.batchGetItem(
          Map(
            companion.tableName ->
              new KeysAndAttributes()
              .withKeys(
                keys._1.asJavaCollection
              )
          )
        ) flatMap { result =>
          builder ++= result.getResponses.get(companion.tableName).asScala.view map { item =>
            companion.fromAttributeMap(item.asScala)
          }
          if (keys._2.isEmpty)
            Future.successful(())
          else
            local(keys._2.splitAt(100))
        }

      local(keys.splitAt(100)) map { _ => builder.result }
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

  def batchDump[T](
    objs: Seq[DynamoDBObject[T]]
  ): Future[Unit] = {
    val tableName = objs.head.companion.tableName

    def local(objsP: (Seq[DynamoDBObject[T]], Seq[DynamoDBObject[T]])): Future[Unit] =
      client.batchWriteItem(
        new BatchWriteItemRequest()
        .withRequestItems(
          Map(
            tableName -> objsP._1.view.map { obj =>
              new WriteRequest()
              .withPutRequest(
                new PutRequest()
                .withItem(obj.toAttributeMap.asJava)
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

  def batchDumpAny(
    objs: Seq[DynamoDBObject[_]]
  ): Future[Unit] = {
    def local(objsP: (Seq[DynamoDBObject[_]], Seq[DynamoDBObject[_]])): Future[Unit] =
      client.batchWriteItem(
        new BatchWriteItemRequest()
        .withRequestItems(
          objsP._1.groupBy(_.companion.tableName).map { case (tableName, objs) =>
            (
              tableName,
              objs.view.map { obj =>
                new WriteRequest()
                .withPutRequest(
                  new PutRequest()
                  .withItem(obj.toAttributeMap.asJava)
                )
              } .asJava
            )
          } .asJava
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
