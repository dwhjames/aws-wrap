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

package com.github.dwhjames.awswrap.dynamodb

import scala.collection.JavaConverters._

import com.amazonaws.services.dynamodbv2.model.{ComparisonOperator, Condition}

private[dynamodb] trait AttributeConditions {

  /**
    * @see [[http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Condition.html DynamoDB API Reference]]
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/Condition.html AWS Java SDK]]
    */
  def equalTo[K](attrVal: K)(implicit conv: K => AttributeValue): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.EQ)
    .withAttributeValueList(conv(attrVal))

  /**
    * @see [[http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Condition.html DynamoDB API Reference]]
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/Condition.html AWS Java SDK]]
    */
  def lessThan[K](attrVal: K)(implicit conv: K => AttributeValue): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.LT)
    .withAttributeValueList(conv(attrVal))

  /**
    * @see [[http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Condition.html DynamoDB API Reference]]
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/Condition.html AWS Java SDK]]
    */
  def lessThanOrEqual[K](attrVal: K)(implicit conv: K => AttributeValue): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.LE)
    .withAttributeValueList(conv(attrVal))

  /**
    * @see [[http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Condition.html DynamoDB API Reference]]
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/Condition.html AWS Java SDK]]
    */
  def greaterThan[K](attrVal: K)(implicit conv: K => AttributeValue): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.GT)
    .withAttributeValueList(conv(attrVal))

  /**
    * @see [[http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Condition.html DynamoDB API Reference]]
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/Condition.html AWS Java SDK]]
    */
  def greaterThanOrEqual[K](attrVal: K)(implicit conv: K => AttributeValue): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.GE)
    .withAttributeValueList(conv(attrVal))

  /**
    * @see [[http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Condition.html DynamoDB API Reference]]
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/Condition.html AWS Java SDK]]
    */
  def beginsWith(attrVal: String): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.BEGINS_WITH)
    .withAttributeValueList(new AttributeValue().withS(attrVal))

  /**
    * @see [[http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Condition.html DynamoDB API Reference]]
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/Condition.html AWS Java SDK]]
    */
  def beginsWith(attrVal: Array[Byte]): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.BEGINS_WITH)
    .withAttributeValueList(new AttributeValue().withB(java.nio.ByteBuffer.wrap(attrVal)))

  /**
    * @see [[http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Condition.html DynamoDB API Reference]]
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/Condition.html AWS Java SDK]]
    */
  def between[K](lower: K, upper: K)(implicit conv: K => AttributeValue): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.BETWEEN)
    .withAttributeValueList(conv(lower), conv(upper))

}

/**
  * A factory of Conditions for queries.
  *
  * @see [[http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Condition.html DynamoDB API Reference]]
  * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/Condition.html AWS Java SDK]]
  */
object QueryCondition extends AttributeConditions

/**
  * A factory of Conditions for scans.
  *
  * @see [[http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Condition.html DynamoDB API Reference]]
  * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/Condition.html AWS Java SDK]]
  */
object ScanCondition extends AttributeConditions {

  /**
    * @see [[http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Condition.html DynamoDB API Reference]]
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/Condition.html AWS Java SDK]]
    */
  def notEqualTo[K](attrVal: K)(implicit conv: K => AttributeValue): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.NE)
    .withAttributeValueList(conv(attrVal))

  /**
    * @see [[http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Condition.html DynamoDB API Reference]]
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/Condition.html AWS Java SDK]]
    */
  val isNull: Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.NULL)

  /**
    * @see [[http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Condition.html DynamoDB API Reference]]
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/Condition.html AWS Java SDK]]
    */
  val notNull: Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.NOT_NULL)

  /**
    * @see [[http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Condition.html DynamoDB API Reference]]
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/Condition.html AWS Java SDK]]
    */
  def contains[K](attrVal: K)(implicit conv: K => AttributeValue): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.CONTAINS)
    .withAttributeValueList(conv(attrVal))

  /**
    * @see [[http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Condition.html DynamoDB API Reference]]
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/Condition.html AWS Java SDK]]
    */
  def doesNotContain[K](attrVal: K)(implicit conv: K => AttributeValue): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.NOT_CONTAINS)
    .withAttributeValueList(conv(attrVal))

  /**
    * @see [[http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Condition.html DynamoDB API Reference]]
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/Condition.html AWS Java SDK]]
    */
  def in[K](attrVals: K*)(implicit conv: K => AttributeValue): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.IN)
    .withAttributeValueList(attrVals.map(conv).asJavaCollection)

  /**
    * @see [[http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Condition.html DynamoDB API Reference]]
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/Condition.html AWS Java SDK]]
    */
  def in[K](attrVals: Set[K])(implicit conv: K => AttributeValue): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.IN)
    .withAttributeValueList(attrVals.map(conv).asJavaCollection)
}
