
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

  def deleteByKey[T](hashKey: Any)(implicit serializer: DynamoDBSerializer[T]): Future[T] =
    client.deleteItem(
      new DeleteItemRequest()
      .withTableName(serializer.tableName)
      .withKey(serializer.makeKey(hashKey).asJava)
      .withReturnValues(ReturnValue.ALL_OLD)
    ) map { result =>
      serializer.fromAttributeMap(result.getAttributes.asScala)
    }

  def deleteByKey[T](
    hashKey:   Any,
    rangeKey:  Any
  )(implicit serializer: DynamoDBSerializer[T]): Future[T] =
    client.deleteItem(
      new DeleteItemRequest()
      .withTableName(serializer.tableName)
      .withKey(serializer.makeKey(hashKey, rangeKey).asJava)
      .withReturnValues(ReturnValue.ALL_OLD)
    ) map { result =>
      serializer.fromAttributeMap(result.getAttributes.asScala)
    }

  def delete[T](
    obj: T
  )(implicit serializer: DynamoDBSerializer[T]): Future[Unit] =
    client.deleteItem(
      tableName = serializer.tableName,
      key       = serializer.primaryKeyOf(obj)
    ) map { _ => () }

  def dump[T](
    obj: T
  )(implicit serializer: DynamoDBSerializer[T]): Future[Unit] =
    client.putItem(
      tableName = serializer.tableName,
      item      = serializer.toAttributeMap(obj)
    ) map { _ => () }

  def loadByKey[T](
    hashKey:   Any
  )(implicit serializer: DynamoDBSerializer[T]): Future[T] =
    client.getItem(
      tableName = serializer.tableName,
      key       = serializer.makeKey(hashKey)
    ) map { result =>
      serializer.fromAttributeMap(result.getItem.asScala)
    }

  def loadByKey[T](
    hashKey:   Any,
    rangeKey:  Any
  )(implicit serializer: DynamoDBSerializer[T]): Future[T] =
    client.getItem(
      tableName = serializer.tableName,
      key       = serializer.makeKey(hashKey, rangeKey)
    ) map { result =>
      serializer.fromAttributeMap(result.getItem.asScala)
    }

  def scan[T](
    scanFilter: Map[String, Condition] = Map.empty
  )(implicit serializer: DynamoDBSerializer[T]): Future[Seq[T]] = {
    val jScanFilter = scanFilter.asJava
    val builder = Seq.newBuilder[T]

    def local(lastKey: Option[JMap[String, AttributeValue]]): Future[Unit] =
      client.scan(
        new ScanRequest()
        .withTableName(serializer.tableName)
        .withScanFilter(jScanFilter)
        .withExclusiveStartKey(lastKey.orNull)
      ) flatMap { result =>
        builder ++= result.getItems.asScala.view map { item =>
          serializer.fromAttributeMap(item.asScala)
        }
        Option { result.getLastEvaluatedKey } match {
          case None   => Future.successful(())
          case optKey => local(optKey)
        }
      }

    local(None) map { _ => builder.result }
  }

  def query[T](
    keyConditions: Map[String, Condition]
  )(implicit serializer: DynamoDBSerializer[T]): Future[Seq[T]] = {
    val jKeyConditions = keyConditions.asJava
    val builder = Seq.newBuilder[T]

    def local(lastKey: Option[JMap[String, AttributeValue]]): Future[Unit] =
      client.query(
        new QueryRequest()
        .withTableName(serializer.tableName)
        .withKeyConditions(jKeyConditions)
        .withExclusiveStartKey(lastKey.orNull)
      ) flatMap { result =>
        builder ++= result.getItems.asScala.view map { item =>
          serializer.fromAttributeMap(item.asScala)
        }
        Option { result.getLastEvaluatedKey } match {
          case None   => Future.successful(())
          case optKey => local(optKey)
        }
      }

    local(None) map { _ => builder.result }
  }


  def batchLoadByKeys[T](
    hashKeys:  Seq[Any],
    rangeKeys: Seq[Any] = Seq.empty
  )(implicit serializer: DynamoDBSerializer[T]): Future[Seq[T]] =
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
            serializer.tableName ->
              new KeysAndAttributes()
              .withKeys(
                keys._1.asJavaCollection
              )
          )
        ) flatMap { result =>
          builder ++= result.getResponses.get(serializer.tableName).asScala.view map { item =>
            serializer.fromAttributeMap(item.asScala)
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

  def batchDump[T](objs: Seq[T])(implicit serializer: DynamoDBSerializer[T]): Future[Unit] = {
    val tableName = serializer.tableName

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

  /* Deprecated for now due to implicit rewrite.
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
  } */
}
