package aws.wrap.dynamodb

import java.util.{Map => JMap, List => JList}

import com.amazonaws.services.dynamodbv2.model.WriteRequest

/**
  * A batch write operation failed after retry.
  *
  * @param message
  *     the exception message
  * @param unprocessedItems
  *     a collection of items that were not written to DynamoDB
  */
class BatchDumpException(message: String, val unprocessedItems: JMap[String, JList[WriteRequest]]) extends RuntimeException(message)

/**
  * A value could not be converted to an [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/AttributeValue.html AttributeValue]]
  *
  * @param message
  *     the exception message
  * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/AttributeValue.html AWS Java SDK]]
  */
class AttributeValueConversionException(message: String) extends RuntimeException(message)
