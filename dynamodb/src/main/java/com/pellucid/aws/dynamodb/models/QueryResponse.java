package com.pellucid.aws.dynamodb.models;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import akka.dispatch.Mapper;

import com.pellucid.aws.internal.AWSJavaConversions;

import play.libs.Scala;
import scala.collection.JavaConversions;

public class QueryResponse {

    private aws.dynamodb.models.QueryResponse scalaResponse;

    private QueryResponse(aws.dynamodb.models.QueryResponse scalaResponse) {
        this.scalaResponse = scalaResponse;
    }

    private List<Map<String, AttributeValue>> items = null;

    public List<Map<String, AttributeValue>> items() {
        if (items == null) {
            items = new ArrayList<Map<String, AttributeValue>>();
            for (aws.dynamodb.Item item: JavaConversions.asJavaIterable(this.scalaResponse.items())) {
                items.add(convertItem(item.toMap()));
            }
        }
        return items;
    }

    public Map<String, AttributeValue> itemAt(int position) {
        return items().get(position);
    }

    public Long count() {
        return (Long)Scala.orNull(this.scalaResponse.count());
    }

    public Long scannedCount() {
        return (Long)Scala.orNull(this.scalaResponse.scannedCount());
    }

    public KeyValue lastEvaluatedKey() {
        return KeyValue.fromScala(Scala.orNull(this.scalaResponse.lastEvaluatedKey()));
    }

    public BigDecimal consumedCapacityUnits() {
        return this.scalaResponse.consumedCapacityUnits().underlying();
    }

    public static QueryResponse fromScala(aws.dynamodb.models.QueryResponse scalaResponse) {
        return new QueryResponse(scalaResponse);
    }

    private static Map<String, AttributeValue> convertItem(scala.collection.Map<String, aws.dynamodb.DDBAttribute> sItem) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        for (String key: JavaConversions.asJavaIterable(sItem.keys())) {
            item.put(key, AttributeValue.fromScala(sItem.apply(key)));
        }
        return item;
    }

}
