package com.pellucid.aws.sqs;

import play.libs.Scala;

public class MessageVisibility {

    private String id;

    private String receiptHandle;

    private Long visibilityTimeout;

    public MessageVisibility(String id, String receiptHandle) {
        this(id, receiptHandle, null);
    }

    public MessageVisibility(String id, String receiptHandle, Long visibilityTimeout) {
        this.id = id;
        this.receiptHandle = receiptHandle;
        this.visibilityTimeout = visibilityTimeout;
    }

    public aws.sqs.MessageVisibility toScala() {
        return new aws.sqs.MessageVisibility(id, receiptHandle, Scala.Option((Object)visibilityTimeout));
    }

}
