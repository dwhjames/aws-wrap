package com.pellucid.aws.sns;

import java.util.List;

import play.libs.Scala;
import scala.collection.JavaConversions;
import akka.dispatch.Mapper;

import com.pellucid.aws.utils.Lists;

public class SubscriptionList {

    private aws.sns.SubscriptionList sList;

    private SubscriptionList(aws.sns.SubscriptionList sList) {
        this.sList = sList;
    }

    public static SubscriptionList fromScala(aws.sns.SubscriptionList sList) {
        return new SubscriptionList(sList);
    }

    public List<Subscription> subscriptions() {
        List<aws.sns.Subscription> sSubs = JavaConversions.seqAsJavaList(this.sList.subscriptions());
        return Lists.map(sSubs, new Mapper<aws.sns.Subscription, Subscription>(){
            @Override public Subscription apply(aws.sns.Subscription sSub) {
                return Subscription.fromScala(sSub);
            }
        });
    }

    public String nextToken() {
        return Scala.orNull(this.sList.nextToken());
    }

    public static class Subscription {
        private String topicArn;
        private String subscriptionArn;
        private String owner;
        private Endpoint endpoint;

        public Subscription(String topicArn, String subscriptionArn, String owner, Endpoint endpoint) {
            this.topicArn = topicArn;
            this.subscriptionArn = subscriptionArn;
            this.owner = owner;
            this.endpoint = endpoint;
        }

        public static Subscription fromScala(aws.sns.Subscription sSubscription) {
            return new Subscription(
                    sSubscription.topicArn(),
                    sSubscription.subscriptionArn(),
                    sSubscription.owner(),
                    endpointFromScala(sSubscription.endpoint())
                    );
        }
    }

    private static Endpoint endpointFromScala(aws.sns.Endpoint sEndpoint) {
        return new Endpoint(sEndpoint.protocol(), sEndpoint.value());
    }

}
