package com.pellucid.aws.sns;

import scala.collection.JavaConversions;
import scala.concurrent.Future;
import scala.runtime.BoxedUnit;

import java.util.List;

import akka.dispatch.Mapper;

import com.pellucid.aws.internal.AWSJavaConversions;
import com.pellucid.aws.utils.Lists;
import com.pellucid.aws.results.Result;
import com.pellucid.aws.sns.SNSRegion;

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
/*
    public Future<Result<SNSMeta, SubscriptionResult>> confirmSubscription(
            String topicArn,
            String token) {
        return confirmSubscription(topicArn, token, false);
    }
*/
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
/*    public Future<Result<SNSMeta, SubscriptionResult>> confirmSubscription(
            String topicArn,
            String token,
            boolean authenticateOnUnsubscribe) {
        return AWSJavaConversions.toJavaResultFuture(aws.sns.SNS.confirmSubscription(topicArn, token, authenticateOnUnsubscribe),
                new MetadataConvert(),
                new Mapper<aws.sns.SubscriptionResult, SubscriptionResult>() {
            @Override public SubscriptionResult apply(aws.sns.SubscriptionResult result) {
                return SubscriptionResult.fromScala(result);
            }
        });
    }
*/
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
