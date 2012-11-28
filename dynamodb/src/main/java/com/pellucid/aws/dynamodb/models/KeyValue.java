package com.pellucid.aws.dynamodb.models;

public class KeyValue {

    private final AttributeValue hashKeyElement;

    private final AttributeValue rangeKeyElement;

    private KeyValue(AttributeValue hashKeyValue, AttributeValue rangeKeyValue) {
        this.hashKeyElement = hashKeyValue;
        this.rangeKeyElement = rangeKeyValue;
    }

    public static KeyValue hashKey(AttributeValue hashKeyValue) {
        return new KeyValue(hashKeyValue, null);
    }

    public static KeyValue compositeKey(AttributeValue hashKeyValue, AttributeValue rangeKeyValue) {
        return new KeyValue(hashKeyValue, rangeKeyValue);
    }

    public AttributeValue hashKeyElement() {
        return hashKeyElement;
    }

    public AttributeValue rangeKeyElement() {
        return rangeKeyElement;
    }

    public aws.dynamodb.models.KeyValue toScala() {
        if (rangeKeyElement == null) {
            return new aws.dynamodb.models.HashKeyValue(hashKeyElement.toScala());
        } else {
            return new aws.dynamodb.models.CompositeKeyValue(hashKeyElement.toScala(), rangeKeyElement.toScala());
        }
    }

}
