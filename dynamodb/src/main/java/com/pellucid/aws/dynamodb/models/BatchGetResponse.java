package com.pellucid.aws.dynamodb.models;

public class BatchGetResponse {

    private aws.dynamodb.BatchGetResponse sResponse;

    private BatchGetResponse(aws.dynamodb.BatchGetResponse sResponse) {
        this.sResponse = sResponse;
    }

    public static BatchGetResponse fromScala(aws.dynamodb.BatchGetResponse sResponse) {
        return new BatchGetResponse(sResponse);
    }

}
