package com.pellucid.aws.dynamodb.models;

import java.util.ArrayList;
import java.util.List;

import scala.collection.JavaConversions;

import play.libs.Scala;

public class Query {

    private String tableName;

    private AttributeValue hashKeyValue;

    private List<String> attributesToGet;

    private Long limit;

    private boolean consistentRead;

    private boolean count;

    private boolean scanIndexForward;

    private KeyCondition rangeKeyCondition;

    private PrimaryKey exclusiveStartKey;

    public Query(String tableName) {
        this(tableName, null, new ArrayList<String>(), null, false, false, true, null, null);
    }

    private Query(
            String tableName,
            AttributeValue hashKeyValue,
            List<String> attributesToGet,
            Long limit,
            Boolean consistentRead,
            Boolean count,
            Boolean scanIndexForward,
            KeyCondition rangeKeyCondition,
            PrimaryKey exclusiveStartKey
            ) {
        this.tableName = tableName;
        this.hashKeyValue = hashKeyValue;
        this.attributesToGet = new ArrayList<String>(attributesToGet);
        this.limit = limit;
        this.consistentRead = consistentRead;
        this.count = count;
        this.rangeKeyCondition = rangeKeyCondition;
        this.exclusiveStartKey = exclusiveStartKey;
    }

    public Query withHashKeyValue(AttributeValue hashKeyValue) {
        return new Query(
                this.tableName,
                hashKeyValue,
                this.attributesToGet,
                this.limit,
                this.consistentRead,
                this.count,
                this.scanIndexForward,
                this.rangeKeyCondition,
                this.exclusiveStartKey
                );
    }

    public Query withAttributesToGet(List<String> attributesToGet) {
        return new Query(
                this.tableName,
                this.hashKeyValue,
                attributesToGet,
                this.limit,
                this.consistentRead,
                this.count,
                this.scanIndexForward,
                this.rangeKeyCondition,
                this.exclusiveStartKey
                );
    }

    public Query withLimit(Long limit) {
        return new Query(
                this.tableName,
                this.hashKeyValue,
                this.attributesToGet,
                limit,
                this.consistentRead,
                this.count,
                this.scanIndexForward,
                this.rangeKeyCondition,
                this.exclusiveStartKey
                );
    }

    public Query withConsistentRead(Boolean consistentRead) {
        return new Query(
                this.tableName,
                this.hashKeyValue,
                this.attributesToGet,
                this.limit,
                consistentRead,
                this.count,
                this.scanIndexForward,
                this.rangeKeyCondition,
                this.exclusiveStartKey
                );
    }

    public Query withCount(Boolean count) {
        return new Query(
                this.tableName,
                this.hashKeyValue,
                this.attributesToGet,
                this.limit,
                this.consistentRead,
                count,
                this.scanIndexForward,
                this.rangeKeyCondition,
                this.exclusiveStartKey
                );
    }

    public Query withRangeKeyCondition(KeyCondition rangeKeyCondition) {
        return new Query(
                this.tableName,
                this.hashKeyValue,
                this.attributesToGet,
                this.limit,
                this.consistentRead,
                this.count,
                this.scanIndexForward,
                rangeKeyCondition,
                this.exclusiveStartKey
                );
    }

    public Query withExclusiveStartKey(PrimaryKey exclusiveStartKey) {
        return new Query(
                this.tableName,
                this.hashKeyValue,
                this.attributesToGet,
                this.limit,
                this.consistentRead,
                this.count,
                this.scanIndexForward,
                this.rangeKeyCondition,
                exclusiveStartKey
                );
    }

    public aws.dynamodb.Query toScala() {
        return new aws.dynamodb.Query(
                this.tableName,
                this.hashKeyValue.toScala(),
                JavaConversions.iterableAsScalaIterable(this.attributesToGet).toSeq(),
                Scala.Option((Object)this.limit),
                this.consistentRead,
                this.count,
                Scala.Option(this.rangeKeyCondition == null ? null : this.rangeKeyCondition.toScala()),
                this.scanIndexForward,
                Scala.Option(this.exclusiveStartKey == null ? null : this.exclusiveStartKey.toScala())
                );
    }

}
