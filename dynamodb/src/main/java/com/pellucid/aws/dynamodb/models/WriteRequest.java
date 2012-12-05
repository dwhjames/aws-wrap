package com.pellucid.aws.dynamodb.models;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.collection.JavaConversions;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import akka.dispatch.Mapper;

import com.pellucid.aws.internal.AWSJavaConversions;

public class WriteRequest {

    public enum Type {
        Put, Delete
    }

    private aws.dynamodb.WriteRequest sRequest;

    private WriteRequest(aws.dynamodb.WriteRequest sRequest) {
        this.sRequest = sRequest;
    }

    public Type type() {
        if (this.sRequest instanceof aws.dynamodb.PutRequest) {
            return Type.Put;
        }
        return Type.Delete;
    }

    public Map<String, AttributeValue> putItem() {
        if (this.type() == Type.Delete) throw new RuntimeException("Not a put request");
        aws.dynamodb.PutRequest sPut = (aws.dynamodb.PutRequest)this.sRequest;
        Map<String, AttributeValue> result = new HashMap<String, AttributeValue>();
        for (String key: JavaConversions.asJavaIterable(sPut.item().keys())) {
            result.put(key, AttributeValue.fromScala(sPut.item().apply(key)));
        }
        return result;
    }

    public KeyValue deleteKey() {
        if (this.type() == Type.Delete) throw new RuntimeException("Not a delete request");
        throw new NotImplementedException();
    }

    public static WriteRequest put(Map<String, AttributeValue> item) {
        return new WriteRequest(new aws.dynamodb.PutRequest(
                AWSJavaConversions.toScalaMap(item, new Mapper<AttributeValue, aws.dynamodb.DDBAttribute>(){
                    @Override
                    public aws.dynamodb.DDBAttribute apply(AttributeValue attr) {
                        return attr.toScala();
                    }
                })
                ));
    }

    public static WriteRequest delete(KeyValue key) {
        return new WriteRequest(new aws.dynamodb.DeleteRequest(key.toScala()));
    }

    public static WriteRequest fromScala(aws.dynamodb.WriteRequest sRequest) {
        return new WriteRequest(sRequest);
    }

    public aws.dynamodb.WriteRequest toScala() {
        return this.sRequest;
    }

    public static scala.collection.immutable.Map<String, scala.collection.Seq<aws.dynamodb.WriteRequest>> requestMapToScala(Map<String, List<WriteRequest>> requests) {
        return AWSJavaConversions.toScalaMap(
                requests,
                new Mapper<List<WriteRequest>, scala.collection.Seq<aws.dynamodb.WriteRequest>>() {
                    @Override
                    public scala.collection.Seq<aws.dynamodb.WriteRequest> apply(List<WriteRequest> requests) {
                        List<aws.dynamodb.WriteRequest> srequests = new ArrayList<aws.dynamodb.WriteRequest>();
                        for (WriteRequest request: requests) {
                            srequests.add(request.toScala());
                        }
                        return AWSJavaConversions.toSeq(srequests);
                    }
                });

    }

}
