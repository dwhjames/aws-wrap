package com.pellucid.aws.sns;

import org.codehaus.jackson.JsonNode;

import play.api.libs.json.JsValue;
import play.libs.Scala;

import com.pellucid.aws.utils.Json;

public class TopicAttributes {

    private aws.sns.TopicAttributes sAttributes;

    private TopicAttributes(aws.sns.TopicAttributes sAttributes) {
        this.sAttributes = sAttributes;
    }

    public static TopicAttributes fromScala(aws.sns.TopicAttributes sAttributes) {
        return new TopicAttributes(sAttributes);
    }

    public String topicArn() {
        return this.sAttributes.topicArn();
    }

    public String owner() {
        return this.sAttributes.owner();
    }

    public String displayName() {
        return this.sAttributes.displayName();
    }

    public Integer subscriptionsPending() {
        return this.sAttributes.subscriptionsPending();
    }
    
    public Integer subscriptionsConfirmed() {
        return this.sAttributes.subscriptionsConfirmed();
    }
    
    public Integer subscriptionsDeleted() {
        return this.sAttributes.subscriptionsDeleted();
    }
    
    public JsonNode policy() {
        JsValue json = Scala.orNull(this.sAttributes.policy());
        if (json == null) return null;
        return Json.parse(json.toString());
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
