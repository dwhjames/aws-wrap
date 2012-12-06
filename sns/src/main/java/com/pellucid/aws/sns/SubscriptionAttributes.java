package com.pellucid.aws.sns;

import org.codehaus.jackson.JsonNode;

import play.api.libs.json.JsValue;
import play.libs.Scala;

import com.pellucid.aws.utils.Json;

public class SubscriptionAttributes {

    private aws.sns.SubscriptionAttributes sAttributes;

    private SubscriptionAttributes(aws.sns.SubscriptionAttributes sAttributes) {
        this.sAttributes = sAttributes;
    }

    public static SubscriptionAttributes fromScala(aws.sns.SubscriptionAttributes sAttributes) {
        return new SubscriptionAttributes(sAttributes);
    }

    public String subscriptionArn() {
        return this.sAttributes.subscriptionArn();
    }

    public String topicArn() {
        return this.sAttributes.topicArn();
    }

    public String owner() {
        return this.sAttributes.owner();
    }

    public boolean confirmationWasAuthenticated() {
        return this.sAttributes.confirmationWasAuthenticated();
    }

    public JsonNode deliveryPolicy() {
        JsValue json = Scala.orNull(this.sAttributes.deliveryPolicy());
        if (json == null) return null;
        return Json.parse(json.toString());
    }

    public JsonNode effectiveDeliveryPolicy() {
        JsValue json = Scala.orNull(this.sAttributes.effectiveDeliveryPolicy());
        if (json == null) return null;
        return Json.parse(json.toString());
    }

}
