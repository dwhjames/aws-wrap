package aws.wrap.dynamodb

import java.util.{Map => JMap, List => JList}

import com.amazonaws.services.dynamodbv2.model.WriteRequest

class BatchDumpException(message: String, val unprocessedItems: JMap[String, JList[WriteRequest]]) extends RuntimeException(message)

class AttributeValueConversionException(message: String) extends RuntimeException(message)
