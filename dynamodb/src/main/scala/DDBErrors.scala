package aws.dynamodb

object DDBErrors {

  /**
   * General authentication failure. The client did not correctly sign the request. Consult the signing documentation.
   */
  val ACCESS_DENIED_EXCEPTION = "AccessDeniedException"

  /**
   * The conditional request failed.
   * Example: The expected value did not match what was stored in the system.
   */
  val CONDITIONAL_CHECK_FAILED_EXCEPTION = "ConditionalCheckFailedException"

  /**
   * The request signature does not conform to AWS standards.
   * The signature in the request did not include all of the required components. See HTTP Header Contents.
   */
  val INCOMPLETE_SIGNATURE_EXCEPTION = "IncompleteSignatureException"

  /**
   * Too many operations for a given subscriber.
   * Example: The number of concurrent table requests (cumulative number of tables in the CREATING, DELETING or UPDATING state) exceeds the maximum allowed of 20. The total limit of tables (currently in the ACTIVE state) is 250. N
   */
  val LIMIT_EXCEEDED_EXCEPTION = "LimitExceededException"

  /**
   * Request must contain a valid (registered) AWS Access Key ID.
   * The request did not include the required x-amz-security-token. See Making HTTP Requests to Amazon DynamoDB.
   */
  val MISSING_AUTHENTICATION_TOKEN_EXCEPTION = "MissingAuthenticationTokenException"

  /**
   * You exceeded your maximum allowed provisioned throughput.
   * Example: Your request rate is too high or the request is too large.
   * TODO: Make sure we retry automatically when we get this exception
   */
  val PROVISIONED_THROUGHPUT_EXCEEDED_EXCEPTION = "ProvisionedThroughputExceededException"

  /**
   * The resource which is being attempted to be changed is in use.
   * Example: You attempted to recreate an existing table, or delete a table currently in the CREATING state.
   */
  val RESOURCE_IN_USE_EXCEPTION = "ResourceInUseException"

  /**
   * The resource which is being requested does not exist.
   * Example: Table which is being requested does not exist, or is too early in the CREATING state.
   */
  val RESOURCE_NOT_FOUND_EXCEPTION = "ResourceNotFoundException"

  /**
   * Rate of requests exceeds the allowed throughput.
   * This can be returned by the control plane API (CreateTable, DescribeTable, etc) when they are requested too rapidly.
   */
  val THROTTLING_EXCEPTION = "ThrottlingException"

  /**
   * One or more required parameter values were missing.
   */
  val VALIDATION_EXCEPTION = "ValidationException"

  /**
   * The server encountered an internal error trying to fulfill the request.
   * The server encountered an error while processing your request.
   */
  val INTERNAL_FAILURE = "InternalFailure"

  /**
   * The server encountered an internal error trying to fulfill the request.
   * The server encountered an error while processing your request.
   */
  val INTERNAL_SERVER_ERROR = "InternalServerError"

  /**
   * The service is currently unavailable or busy.
   * There was an unexpected error on the server while processing your request.
   */
  val SERVICE_UNAVAILABLE_EXCEPTION = "ServiceUnavailableException"

}

