package aws.sqs

object SQSErrors {

  val AccessDenied = "AccessDenied"
  val AuthFailure = "AuthFailure"
  val InternalError = "AWS.SimpleQueueService.InternalError"
  val NonExistentQueue = "AWS.SimpleQueueService.NonExistentQueue"
  val SQSInternalError = "InternalError"
  val InvalidAccessKeyId = "InvalidAccessKeyId"
  val InvalidAction = "InvalidAction"
  val InvalidAddress = "InvalidAddress"
  val InvalidHttpRequest = "InvalidHttpRequest"
  val InvalidParameterCombination = "InvalidParameterCombination"
  val InvalidParameterValue = "InvalidParameterValue"
  val InvalidQueryParameter = "InvalidQueryParameter"
  val InvalidRequest = "InvalidRequest"
  val InvalidSecurity = "InvalidSecurity"
  val InvalidSecurityToken = "InvalidSecurityToken"
  val MalformedVersion = "MalformedVersion"
  val MissingClientTokenId = "MissingClientTokenId"
  val MissingCredentials = "MissingCredentials"
  val MissingParameter = "MissingParameter"
  val NoSuchVersion = "NoSuchVersion"
  val NotAuthorizedToUseVersion = "NotAuthorizedToUseVersion"
  val RequestExpired = "RequestExpired"
  val RequestThrottled = "RequestThrottled"
  val ServiceUnavailable = "ServiceUnavailable"
  val X509ParseError = "X509ParseError"

}
