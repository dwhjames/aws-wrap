package com.pellucid.aws.sns;

import java.util.List;

import org.codehaus.jackson.JsonNode;

import play.api.libs.json.Json;
import play.api.libs.json.JsValue;
import play.libs.Scala;
import scala.collection.JavaConversions;
import scala.concurrent.Future;
import scala.runtime.BoxedUnit;
import akka.dispatch.Mapper;

import com.pellucid.aws.internal.AWSJavaConversions;
import com.pellucid.aws.results.Result;
import com.pellucid.aws.utils.Identity;
import com.pellucid.aws.utils.Lists;

public class SNS {

    private final aws.sns.SNSRegion scalaRegion;

    public SNS(SNSRegion region) {
        this.scalaRegion = SNS.scalaRegion(region);
    }

    /**
     * Adds a statement to a topic's access control policy,
     * granting access for the specified AWS accounts to the specified actions.
     *
     * @param topicArn The ARN of the topic whose access control policy you wish to modify.
     * @param label A unique identifier for the new policy statement.
     * @param awsAccounts The AWS account IDs of the users (principals) who will be given access to the specified actions.
     *                    The users must have AWS accounts, but do not need to be signed up for this service.
     * @param actions The action you want to allow for the specified principal(s).
     */
    public Future<Result<SNSMeta, Object>> addPermission(
            String topicArn,
            String label,
            List<String> awsAccounts,
            List<Action> actions) {
        List<aws.sns.Action> sActions = Lists.map(actions, new Mapper<Action, aws.sns.Action>(){
            @Override public aws.sns.Action apply(Action action) {
                return aws.sns.Action$.MODULE$.apply(action.toString());
            }
        });
        return convertEmptyResult(aws.sns.SNS.addPermission(
                topicArn,
                label,
                JavaConversions.iterableAsScalaIterable(awsAccounts).toSeq(),
                JavaConversions.iterableAsScalaIterable(sActions).toSeq(),
                this.scalaRegion));
    }

    public Future<Result<SNSMeta, String>> confirmSubscription(
            String topicArn,
            String token) {
        return confirmSubscription(topicArn, token, false);
    }

    /**
     * Verifies an endpoint owner's intent to receive messages by validating the token
     * sent to the endpoint by an earlier Subscribe action. If the token is valid, the action creates a new subscription and
     * returns its Amazon Resource Name (ARN).
     *
     * @param topicArn The ARN of the topic for which you wish to confirm a subscription.
     * @param token Short-lived token sent to an endpoint during the Subscribe action.
     * @param authenticateOnUnsubscribe Disallows unauthenticated unsubscribes of the subscription.
     *        If the value of this parameter is true and the request has an AWS signature,
     *        then only the topic owner and the subscription owner can unsubscribe the endpoint.
     */
    public Future<Result<SNSMeta, String>> confirmSubscription(
            String topicArn,
            String token,
            boolean authenticateOnUnsubscribe) {
        return convertStringResult(aws.sns.SNS.confirmSubscription(topicArn, token, authenticateOnUnsubscribe, this.scalaRegion));
    }

    /**
     * Deletes a topic and all its subscriptions.
     * Deleting a topic might prevent some messages previously sent to the topic from being delivered to subscribers.
     * This action is idempotent, so deleting a topic that does not exist will not result in an error.
     *
     * @param topicArn The ARN of the topic you want to delete.
     */
    public Future<Result<SNSMeta, Object>> deleteTopic(String topicArn) {
        return convertEmptyResult(aws.sns.SNS.deleteTopic(topicArn, this.scalaRegion));
    }

    /**
     * Creates a topic to which notifications can be published. Users can create at most 100 topics.
     * For more information, see http://aws.amazon.com/sns. This action is idempotent, so if the requester already owns a topic
     * with the specified name, that topic's ARN will be returned without creating a new topic.
     *
     * Constraints: Topic names must be made up of only uppercase and lowercase ASCII letters, numbers, underscores, and hyphens,
     * and must be between 1 and 256 characters long.
     *
     * @param name The name of the topic you want to create.
     */
    public Future<Result<SNSMeta, String>> createTopic(String name) {
        return convertStringResult(aws.sns.SNS.createTopic(name, this.scalaRegion));
    }

    /**
     * Returns all of the properties of a subscription.
     *
     * @param subscriptionArn The ARN of the subscription whose properties you want to get.
     */
    public Future<Result<SNSMeta, SubscriptionAttributes>> getSubscriptionAttributes(String subscriptionArn) {
        return AWSJavaConversions.toJavaResultFuture(aws.sns.SNS.getSubscriptionAttributes(subscriptionArn, scalaRegion),
                new MetadataConvert(),
                new Mapper<aws.sns.SubscriptionAttributes, SubscriptionAttributes>() {
            @Override public SubscriptionAttributes apply(aws.sns.SubscriptionAttributes attributes) {
                return SubscriptionAttributes.fromScala(attributes);
            }
        });
    }

    /**
     * Returns all of the properties of a topic.
     * Topic properties returned might differ based on the authorization of the user.
     *
     * @param topicArn The ARN of the topic whose properties you want to get.
     */
    public Future<Result<SNSMeta, TopicAttributes>> getTopicAttributes(String topicArn) {
        return AWSJavaConversions.toJavaResultFuture(aws.sns.SNS.getTopicAttributes(topicArn, scalaRegion),
                new MetadataConvert(),
                new Mapper<aws.sns.TopicAttributes, TopicAttributes>() {
            @Override public TopicAttributes apply(aws.sns.TopicAttributes attributes) {
                return TopicAttributes.fromScala(attributes);
            }
        });
    }

    public Future<Result<SNSMeta, SubscriptionList>> listSubscriptions() {
        return listSubscriptions(null);
    }

    /**
     * Returns a list of the requester's subscriptions.
     * Each call returns a limited list of subscriptions, up to 100.
     * If there are more subscriptions, a NextToken is also returned.
     * Use the NextToken parameter in a new ListSubscriptions call to get further results.
     *
     * @param nextToken Token returned by the previous `listSubscriptions` request.
     */
    public Future<Result<SNSMeta, SubscriptionList>> listSubscriptions(String nextToken) {
        return AWSJavaConversions.toJavaResultFuture(aws.sns.SNS.listSubscriptions(Scala.Option(nextToken), scalaRegion),
                new MetadataConvert(),
                new Mapper<aws.sns.SubscriptionList, SubscriptionList>() {
            @Override public SubscriptionList apply(aws.sns.SubscriptionList attributes) {
                return SubscriptionList.fromScala(attributes);
            }
        });
    }

    public Future<Result<SNSMeta, SubscriptionList>> listSubscriptionsByTopic(String topicArn) {
        return listSubscriptionsByTopic(topicArn, null);
    }

    /**
     * Returns a list of the subscriptions to a specific topic.
     * Each call returns a limited list of subscriptions, up to 100.
     * If there are more subscriptions, a `nextToken` is also returned.
     * Use the `nextToken` parameter in a new `listSubscriptionsByTopic` call to get further results.
     *
     * @param topicArn The ARN of the topic for which you wish to find subscriptions.
     * @param nextToken Token returned by the previous 'listSubscriptionsByTopic` request.
     */
    public Future<Result<SNSMeta, SubscriptionList>> listSubscriptionsByTopic(String topicArn, String nextToken) {
        return AWSJavaConversions.toJavaResultFuture(aws.sns.SNS.listSubscriptionsByTopic(topicArn, Scala.Option(nextToken),
                scalaRegion),
                new MetadataConvert(),
                new Mapper<aws.sns.SubscriptionList, SubscriptionList>() {
            @Override public SubscriptionList apply(aws.sns.SubscriptionList attributes) {
                return SubscriptionList.fromScala(attributes);
            }
        });
    }

    public Future<Result<SNSMeta, ListTopics>> listTopics() {
        return listTopics(null);
    }

    public Future<Result<SNSMeta, String>> publish(String topicArn, Message message) {
        return publish(topicArn, message, null);
    }

    /**
     * Sends a message to all of a topic's subscribed endpoints.
     * When a `messageId` is returned, the message has been saved and Amazon SNS will attempt to deliver it to the topic's subscribers shortly.
     * The format of the outgoing message to each subscribed endpoint depends on the notification protocol selected.
     *
     * @param topicArn The topic you want to publish to.
     * @param message The message you want to send to the topic.
     * @param subject Optional parameter to be used as the "Subject" line of when the message is delivered to e-mail endpoints.
     *                This field will also be included, if present, in the standard JSON messages delivered to other endpoints.
     *                Constraints: Subjects must be ASCII text that begins with a letter, number or punctuation mark;
     *                must not include line breaks or control characters; and must be less than 100 characters long.
     */
    public Future<Result<SNSMeta, String>> publish(String topicArn,
                Message message,
                String subject) {
        return convertStringResult(aws.sns.SNS.publish(topicArn, message.toScala(), Scala.Option(subject), scalaRegion));
    }

    /**
     * Returns a list of the requester's topics. Each call returns a limited list of topics, up to 100.
     * If there are more topics, a `nextToken` is also returned.
     * Use the `nextToken` parameter in a new `listTopics` call to get further results.
     *
     * @param nextToken Token returned by the previous `listTopics` request.
     */
    public Future<Result<SNSMeta, ListTopics>> listTopics(String nextToken) {
        return AWSJavaConversions.toJavaResultFuture(aws.sns.SNS.listTopics(Scala.Option(nextToken), scalaRegion),
                new MetadataConvert(),
                new Mapper<aws.sns.ListTopics, ListTopics>() {
            @Override public ListTopics apply(aws.sns.ListTopics attributes) {
                return ListTopics.fromScala(attributes);
            }
        });
    }

    /**
     * The RemovePermission action removes a statement from a topic's access control policy.
     *
     * @param topicArn The ARN of the topic whose access control policy you wish to modify.
     * @param label The unique label of the statement you want to remove.
     *
     */
    public Future<Result<SNSMeta, Object>> removePermission(String topicArn, String label) {
        return convertEmptyResult(aws.sns.SNS.removePermission(topicArn, label, scalaRegion));
    }

    public Future<Result<SNSMeta, Object>> setTopicDisplayName(String topicArn, String name) {
        return convertEmptyResult(aws.sns.SNS.setTopicDisplayName(topicArn, name, scalaRegion));
    }

    public Future<Result<SNSMeta, Object>> setTopicPolicy(String topicArn, JsonNode policy) {
        JsValue json = Json.parse(policy.toString());
        return convertEmptyResult(aws.sns.SNS.setTopicPolicy(topicArn, json, scalaRegion));
    }

    public Future<Result<SNSMeta, Object>> setTopicDeliveryPolicy(String topicArn, JsonNode deliveryPolicy) {
        JsValue json = Json.parse(deliveryPolicy.toString());
        return convertEmptyResult(aws.sns.SNS.setTopicDeliveryPolicy(topicArn, json, scalaRegion));
    }

    /**
     * The Subscribe action prepares to subscribe an endpoint by sending the endpoint a confirmation message.
     * To actually create a subscription, the endpoint owner must call the ConfirmSubscription action with the token
     * from the confirmation message. Confirmation tokens are valid for three days.
     *
     * @param endpoint The endpoint that you want to receive notifications.
     * @param topicArn The ARN of topic you want to subscribe to.
     */
    public Future<Result<SNSMeta, String>> subscribe(Endpoint endpoint, String topicArn) {
        return convertStringResult(aws.sns.SNS.subscribe(endpoint.toScala(), topicArn, scalaRegion));
    }

    public Future<Result<SNSMeta, Object>> unsubscribe(String subscriptionArn) {
        return convertEmptyResult(aws.sns.SNS.unsubscribe(subscriptionArn, scalaRegion));
    }

    private static Future<Result<SNSMeta, String>> convertStringResult(Future<aws.core.Result<aws.sns.SNSMeta, String>> scalaResult) {
        return AWSJavaConversions.toJavaResultFuture(scalaResult, new MetadataConvert(), new Identity());
    }

    private static Future<Result<SNSMeta, Object>> convertEmptyResult(Future<aws.core.Result<aws.sns.SNSMeta, BoxedUnit>> scalaResult) {
        return AWSJavaConversions.toJavaResultFuture(scalaResult, new MetadataConvert(), new Mapper<BoxedUnit, Object>() {
            @Override public Object apply(BoxedUnit unit) {
                return null;
            }
        });
    }

    private static aws.sns.SNSRegion scalaRegion(SNSRegion region) {
        switch (region) {
        case US_EAST_1: return aws.sns.SNSRegion$.MODULE$.US_EAST_1();
        case US_WEST_1: return aws.sns.SNSRegion$.MODULE$.US_WEST_1();
        case US_WEST_2: return aws.sns.SNSRegion$.MODULE$.US_WEST_2();
        case EU_WEST_1: return aws.sns.SNSRegion$.MODULE$.EU_WEST_1();
        case ASIA_SOUTHEAST_1: return aws.sns.SNSRegion$.MODULE$.ASIA_SOUTHEAST_1();
        case ASIA_NORTHEAST_1: return aws.sns.SNSRegion$.MODULE$.ASIA_NORTHEAST_1();
        case SA_EAST_1: return aws.sns.SNSRegion$.MODULE$.SA_EAST_1();
        }
        return aws.sns.SNSRegion$.MODULE$.DEFAULT();
    }

    private static class MetadataConvert extends Mapper<aws.sns.SNSMeta, SNSMeta> {
        @Override
        public SNSMeta apply(aws.sns.SNSMeta scalaMeta) {
            return new SNSMeta(scalaMeta.requestId());
        }
    }

}
