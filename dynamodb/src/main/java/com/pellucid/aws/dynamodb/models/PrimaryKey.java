package com.pellucid.aws.dynamodb.models;

public class PrimaryKey {

    private KeySchemaElement hashKeyElement;
    private KeySchemaElement rangeKeyElement;

    public PrimaryKey(KeySchemaElement hashKeyElement, KeySchemaElement rangeKeyElement) {
        if (this.hashKeyElement == null) {
            throw new RuntimeException("hashKeyElement is mandatory, and can not be null");
        }
        this.hashKeyElement = hashKeyElement;
        this.rangeKeyElement = rangeKeyElement;
    }

    public KeySchemaElement hashKeyElement() {
        return this.hashKeyElement;
    }

    public KeySchemaElement rangeKeyElement() {
        return this.rangeKeyElement;
    }
    /*
    public KeySchema fromScala(aws.dynamodb.KeySchema scalaSchema) {
        return new KeySchema()
    }*/

    public aws.dynamodb.models.PrimaryKey toScala() {
        if (rangeKeyElement == null) {
            return new aws.dynamodb.models.CompositeKey(hashKeyElement.toScala(), rangeKeyElement.toScala());
        }
        return new aws.dynamodb.models.HashKey(hashKeyElement.toScala());
    }

}
