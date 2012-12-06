package com.pellucid.aws.dynamodb.models;

import java.util.ArrayList;
import java.util.List;

import akka.dispatch.Mapper;

import com.pellucid.aws.internal.*;

public class GetRequest {

    private aws.dynamodb.GetRequest sRequest;

    private GetRequest(aws.dynamodb.GetRequest sRequest) {
        this.sRequest = sRequest;
    }

    public GetRequest(String tableName, List<KeyValue> keys, boolean consistentRead) {
        this(tableName, keys, new ArrayList<String>(), consistentRead);
    }

    public GetRequest(String tableName, List<KeyValue> keys) {
        this(tableName, keys, new ArrayList<String>(), false);
    }

    public GetRequest(String tableName, List<KeyValue> keys, List<String> attributesToGet) {
        this(tableName, keys, attributesToGet, false);
    }

    public GetRequest(String tableName, List<KeyValue> keys, List<String> attributesToGet, boolean consistentRead) {
        scala.collection.Seq<aws.dynamodb.KeyValue> sKeys = AWSJavaConversions.toSeq(Lists.map(keys, new Mapper<KeyValue, aws.dynamodb.KeyValue>(){
            @Override public aws.dynamodb.KeyValue apply(KeyValue kv) {
                return kv.toScala();
            }
        }));
        this.sRequest = new aws.dynamodb.GetRequest(tableName, sKeys, AWSJavaConversions.toSeq(attributesToGet), consistentRead);
    }

    public GetRequest fromScala(aws.dynamodb.GetRequest sRequest) {
        return new GetRequest(sRequest);
    }

    public aws.dynamodb.GetRequest toScala() {
        return this.sRequest;
    }

}
