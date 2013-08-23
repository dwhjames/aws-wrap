package aws.wrap.dynamodb


/**
  * A value could not be converted to an [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/AttributeValue.html AttributeValue]]
  *
  * @param message
  *     the exception message
  * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/AttributeValue.html AWS Java SDK]]
  */
class AttributeValueConversionException(message: String) extends RuntimeException(message)
