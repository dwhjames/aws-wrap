package com.pellucid.aws.simpledb;

public class SimpleDBMeta {

    private final String mRequestId;
    private final String mBoxUsage;

    public SimpleDBMeta(String requestId, String boxUsage) {
        this.mRequestId = requestId;
        this.mBoxUsage = boxUsage;
    }

    public String requestId() {
        return this.mRequestId;
    }

    public String boxUsage() {
        return this.mBoxUsage;
    }

}
