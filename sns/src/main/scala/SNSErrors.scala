/*
 * Copyright 2012 Pellucid and Zenexity
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

package aws.sns

object SNSErrors {

  /**
   * The request signature does not conform to AWS standards.
   */
  val INCOMPLETE_SIGNATURE = "IncompleteSignature"

  /**
   * The request processing has failed due to some unknown error, exception or failure.
   */
  val INTERVAL_FAILURE = "InternalFailure"

  /**
   * The action or operation requested is invalid.
   */
  val INVALID_ACTION = "InvalidAction"

  /**
   * The X.509 certificate or AWS Access Key ID provided does not exist in our records.
   */
  val INVALID_CLIENT_TOKEN_ID = "InvalidClientTokenId"

  /**
   * Parameters that must not be used together were used together.
   */
  val INVALID_PARAMETER_COMBINATION = "InvalidParameterCombination"

  /**
   * Bad parameter. See the message for details about which parameter is bad.
   */
  val INVALID_PARAMETER = "InvalidParameter"

  /**
   * A bad or out-of-range value was supplied for the input parameter.
   */
  val INVALID_PARAMETER_VALUE = "InvalidParameterValue"

  /**
   * AWS query string is malformed, does not adhere to AWS standards.
   */
  val INVALID_QUERY_PARAMETER = "InvalidQueryParameter"

  /**
   * The query string is malformed.
   */
  val MALFORMED_QUERYSTRING = "MalformedQueryString"

  /**
   * The request is missing an action or operation parameter.
   */
  val MISSING_ACTION = "MissingAction"

  /**
   * Request must contain either a valid (registered) AWS Access Key ID or X.509 certificate.
   */
  val MISSING_AUTHENTICATION_TOKEN = "MissingAuthenticationToken"

  /**
   * An input parameter that is mandatory for processing the request is not supplied.
   */
  val MISSING_PARAMETER = "MissingParameter"

  /**
   * The AWS Access Key ID needs a subscription for the service.
   */
  val OPT_IN_REQUIRED = "OptInRequired"

  /**
   * Request is past expires date or the request date (either with 15 minute padding), or the request date occurs more than 15 minutes in the future.
   */
  val REQUEST_EXPIRED = "RequestExpired"

  /**
   * The request has failed due to a temporary failure of the server.
   */
  val SERVICE_UNAVAILABLE = "ServiceUnavailable"

  /**
   * Request was denied due to request throttling.
   */
  val THROTTLING = "Throttling"

}

