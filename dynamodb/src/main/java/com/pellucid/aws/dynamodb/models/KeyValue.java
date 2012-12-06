package com.pellucid.aws.dynamodb.models;

import java.nio.ByteBuffer;

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

    public static KeyValue hashKey(String value) {
        return hashKey(AttributeValue.createS(value));
    }

    public static KeyValue hashKey(Double value) {
        return hashKey(AttributeValue.createN(value));
    }

    public static KeyValue hashKey(ByteBuffer value) {
        return hashKey(AttributeValue.createB(value));
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

    public aws.dynamodb.KeyValue toScala() {
        if (rangeKeyElement == null) {
            return new aws.dynamodb.HashKeyValue(hashKeyElement.toScala());
        } else {
            return new aws.dynamodb.CompositeKeyValue(hashKeyElement.toScala(), rangeKeyElement.toScala());
        }
    }

    public static KeyValue fromScala(aws.dynamodb.KeyValue sKeyValue) {
        if (sKeyValue == null) return null;
        AttributeValue rangeKeyElement = null;
        AttributeValue hashKeyElement = AttributeValue.fromScala(sKeyValue.hashKeyElement());
        if (sKeyValue instanceof aws.dynamodb.CompositeKeyValue) {
            rangeKeyElement = AttributeValue.fromScala(((aws.dynamodb.CompositeKeyValue)sKeyValue).rangeKeyElement());
        }
        return new KeyValue(hashKeyElement, rangeKeyElement);
    }

}
