package com.pellucid.aws.sns;

public class SNSMeta {

    private String requestId;

    public SNSMeta(String requestId) {
        this.requestId = requestId;
    }

    public String requestId() {
        return requestId;
    }

}
