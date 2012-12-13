package com.pellucid.aws.sqs;

public class MessageResponse {

    private aws.sqs.MessageResponse scalaResponse;

    private MessageResponse(aws.sqs.MessageResponse scalaResponse) {
        this.scalaResponse = scalaResponse;
    }

    public String id() {
        return scalaResponse.id();
    }

    public String messageId() {
        return scalaResponse.messageId();
    }

    public String md5OfBody() {
        return scalaResponse.md5OfBody();
    }

    public static MessageResponse fromScala(aws.sqs.MessageResponse scalaResponse) {
        return new MessageResponse(scalaResponse);
    }

}
