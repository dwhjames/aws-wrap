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

import com.amazonaws.services.dynamodbv2.model.{
  AttributeDefinition, KeySchemaElement, KeyType,
  ProvisionedThroughput, ScalarAttributeType
}

/**
  * Convenience methods for constructing DynamoDB table schemas.
  *
  * @see [[http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html DynamoDB API Reference]]
  */
object Schema {

  /**
    * Constructs a
    * [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/ProvisionedThroughput.html ProvisionedThroughput]]
    * model.
    *
    * @param readCapacityUnits
    *     the number of read capacity units to provision.
    * @param writeCapacityUnits
    *     the number of write capacity units to provision.
    * @return a ProvisionedThroughput model.
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/AttributeDefinition.html AWS Java SDK]]
    */
  def provisionedThroughput(readCapacityUnits: Long, writeCapacityUnits: Long): ProvisionedThroughput =
    new ProvisionedThroughput()
      .withReadCapacityUnits(readCapacityUnits)
      .withWriteCapacityUnits(writeCapacityUnits)

  /**
    * Constructs an
    * [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/AttributeDefinition.html AttributeDefinition]]
    * model for a string attribute.
    *
    * @param attributeName
    *     the name of the attribute.
    * @return an AttributeDefinition model.
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/AttributeDefinition.html AWS Java SDK]]
    */
  def stringAttribute(attributeName: String): AttributeDefinition =
    new AttributeDefinition()
      .withAttributeName(attributeName)
      .withAttributeType(ScalarAttributeType.S)

  /**
    * Constructs an
    * [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/AttributeDefinition.html AttributeDefinition]]
    * model for a number attribute.
    *
    * @param attributeName
    *     the name of the attribute.
    * @return an AttributeDefinition model.
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/AttributeDefinition.html AWS Java SDK]]
    */
  def numberAttribute(attributeName: String): AttributeDefinition =
    new AttributeDefinition()
      .withAttributeName(attributeName)
      .withAttributeType(ScalarAttributeType.N)

  /**
    * Constructs an
    * [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/AttributeDefinition.html AttributeDefinition]]
    * model for a number attribute.
    *
    * @param attributeName
    *     the name of the attribute.
    * @return an AttributeDefinition model.
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/AttributeDefinition.html AWS Java SDK]]
    */
  def binaryAttribute(attributeName: String): AttributeDefinition =
    new AttributeDefinition()
      .withAttributeName(attributeName)
      .withAttributeType(ScalarAttributeType.B)

  /**
    * Constructs a
    * [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/KeySchemaElement.html KeySchemaElement]]
    * model for a hash key.
    *
    * @param attributeName
    *     the name of the attribute.
    * @return a KeySchemaElement model.
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/KeySchemaElement.html AWS Java SDK]]
    */
  def hashKey(attributeName: String): KeySchemaElement =
    new KeySchemaElement()
      .withAttributeName(attributeName)
      .withKeyType(KeyType.HASH)

  /**
    * Constructs a
    * [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/KeySchemaElement.html KeySchemaElement]]
    * model for a range key.
    *
    * @param attributeName
    *     the name of the attribute.
    * @return a KeySchemaElement model.
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/KeySchemaElement.html AWS Java SDK]]
    */
  def rangeKey(attributeName: String): KeySchemaElement =
    new KeySchemaElement()
      .withAttributeName(attributeName)
      .withKeyType(KeyType.RANGE)

}
