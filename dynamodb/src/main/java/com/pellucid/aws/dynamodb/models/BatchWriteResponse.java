package com.pellucid.aws.dynamodb.models;

import java.util.Map;

import scala.Predef;
import scala.Tuple2;

import akka.dispatch.Mapper;

import com.pellucid.aws.internal.AWSJavaConversions;

public class BatchWriteResponse {

    private aws.dynamodb.BatchWriteResponse sResponse;

    private BatchWriteResponse(aws.dynamodb.BatchWriteResponse sResponse) {
        this.sResponse = sResponse;
    }

    public Map<String, QueryResponse> responses() {
        return AWSJavaConversions.toJavaMap(sResponse.responses().toMap(
                Predef.<Tuple2<String, aws.dynamodb.models.QueryResponse>>conforms()
                ), new Mapper<aws.dynamodb.models.QueryResponse, QueryResponse>(){
            @Override
            public QueryResponse apply(aws.dynamodb.models.QueryResponse sResponse) {
                return QueryResponse.fromScala(sResponse);
            }
        });
    }

    public static BatchWriteResponse fromScala(aws.dynamodb.BatchWriteResponse sResponse) {
        return new BatchWriteResponse(sResponse);
    }

}
