package com.github.dwhjames.awswrap.dynamodb

import com.amazonaws.services.dynamodbv2.model.{Condition, QueryRequest}
import scala.collection.JavaConverters._

trait QueryBuilder {
  /**
    * A convienience type synonym for
    * [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/AttributeValue.html AttributeValue]].
    */
  type AttributeValue = com.amazonaws.services.dynamodbv2.model.AttributeValue

  /**
    * Constructs a
    * [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/QueryRequest.html QueryRequest]]
    * for a hash key.
    *
    * Constructs a
    * [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/QueryRequest.html QueryRequest]]
    * from an attribute name and attribute value pair.
    *
    * @param hashAttr
    *     the hash key name and the value it must equal.
    * @return a query request
    */
  def mkHashKeyQuery[K](hashAttr: (String, K))
                       (implicit ev: K => AttributeValue): QueryRequest =
    new QueryRequest()
      .withKeyConditions(
        Map(
          hashAttr._1 -> QueryCondition.equalTo(hashAttr._2)
        ).asJava
      )

  /**
    * Constructs a
    * [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/QueryRequest.html QueryRequest]]
    * for a hash key.
    *
    * Constructs a
    * [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/QueryRequest.html QueryRequest]]
    * from an attribute value and an implicit serializer.
    *
    * @param hashValue
    *     the value the hash key must equal.
    * @param serializer
    *     an implicit object serializer
    * @return a query request
    */
  def mkHashKeyQuery[T, K](hashValue: K)
                          (implicit serializer: DynamoDBSerializer[T], ev: K => AttributeValue): QueryRequest =
    mkHashKeyQuery(serializer.hashAttributeName -> hashValue)

  /**
    * Constructs a
    * [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/QueryRequest.html QueryRequest]]
    * for hash and range keys.
    *
    * Constructs a
    * [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/QueryRequest.html QueryRequest]]
    * from an attribute name and attribute value pair, and an attribute name
    * and condition pair.
    *
    * @param hashAttr
    *     the hash key name and the value it must equal.
    * @param rangeAttr
    *     an attribute name and condition pair.
    * @return a query request
    * @see [[QueryCondition]]
    */
  def mkHashAndRangeKeyQuery[K](hashAttr: (String, K), rangeAttr: (String, Condition))
                               (implicit ev: K => AttributeValue): QueryRequest =
    new QueryRequest()
      .withKeyConditions(
        Map(
          hashAttr._1  -> QueryCondition.equalTo(hashAttr._2),
          rangeAttr._1 -> rangeAttr._2
        ).asJava
      )

  /**
    * Constructs a
    * [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/QueryRequest.html QueryRequest]]
    * for hash and range keys.
    *
    * Constructs a
    * [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/QueryRequest.html QueryRequest]]
    * from an attribute value, a range condition, and an implicit serializer.
    *
    * @param hashValue
    *     the value the hash key must equal.
    * @param rangeCondition
    *     a condition on the range key.
    * @param serializer
    *     an implicit object serializer
    * @return a query request
    * @see [[QueryCondition]]
    */
  def mkHashAndRangeKeyQuery[T, K](hashValue: K, rangeCondition: Condition)
                                  (implicit serializer: DynamoDBSerializer[T],  ev: K => AttributeValue): QueryRequest =
    mkHashAndRangeKeyQuery(
      serializer.hashAttributeName -> hashValue,
      serializer.rangeAttributeName.getOrElse {
        throw new UnsupportedOperationException(s"mkHashAndRangeKeyQuery: table ${serializer.tableName} does not have a range key")
      } -> rangeCondition
    )
}
