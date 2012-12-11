package com.pellucid.aws.sqs;

import java.util.HashMap;
import java.util.Map;

import scala.collection.JavaConversions;

public class MessageReceive {

    private aws.sqs.MessageReceive scalaReceive;
    private Map<MessageAttribute, String> attributes;

    private MessageReceive(aws.sqs.MessageReceive scalaReceive) {
        this.scalaReceive = scalaReceive;
    }

    public String messageId() {
        return this.scalaReceive.messageId();
    }

    public String body() {
        return this.scalaReceive.body();
    }

    public String md5OfBody() {
        return this.scalaReceive.md5OfBody();
    }

    public String receiptHandle() {
        return this.scalaReceive.receiptHandle();
    }

    public synchronized Map<MessageAttribute, String> attributes() {
        if (attributes == null) {
            attributes = new HashMap<MessageAttribute, String>();
            for (aws.sqs.MessageAttribute sAttribute: JavaConversions.asJavaIterable(scalaReceive.attributes().keys())) {
                attributes.put(MessageAttribute.valueOf(sAttribute.toString()), scalaReceive.attributes().apply(sAttribute));
            }
        }
        return attributes;
    }

    public String attribute(MessageAttribute key) {
        return attributes().get(key);
    }

    public static MessageReceive fromScala(aws.sqs.MessageReceive scalaReceive) {
        return new MessageReceive(scalaReceive);
    }

}
/*
case class MessageReceive(
        messageId: String,
        body: String,
        md5OfBody: String,
        receiptHandle: String,
        attributes: Map[MessageAttribute, String])
 */