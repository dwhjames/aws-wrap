package com.pellucid.aws.sqs;

public class SQSMeta {

    private String requestId;

    public SQSMeta(String requestId) {
        this.requestId = requestId;
    }

    public String requestId() {
        return requestId;
    }

}
