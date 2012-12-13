package com.pellucid.aws.sqs;

public class MessageDelete {

    private String id;

    private String receiptHandle;

    public MessageDelete(String id, String receiptHandle) {
        this.id = id;
        this.receiptHandle = receiptHandle;
    }

    public aws.sqs.MessageDelete toScala() {
        return new aws.sqs.MessageDelete(id, receiptHandle);
    }

}
