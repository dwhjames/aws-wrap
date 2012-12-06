package com.pellucid.aws.dynamodb.models;

import play.libs.Scala;

public class Expected {

    private final AttributeValue value;

    private final Boolean exists;

    public Expected(AttributeValue attributeValue) {
        this.value = attributeValue;
        this.exists = null;
    }

    public Expected(AttributeValue attributeValue, Boolean exists) {
        this.value = attributeValue;
        this.exists = exists;
    }

    public AttributeValue attributeValue() {
        return value;
    }

    public Boolean exists() {
        return exists;
    }

    public aws.dynamodb.Expected toScala() {
        return new aws.dynamodb.Expected(Scala.Option((Object)exists), Scala.Option(value.toScala()));
    }

}
