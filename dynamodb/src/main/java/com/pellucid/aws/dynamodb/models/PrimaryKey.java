package com.pellucid.aws.dynamodb.models;

public class PrimaryKey {

    private final KeySchemaElement hashKeyElement;
    private final KeySchemaElement rangeKeyElement;

    public PrimaryKey(KeySchemaElement hashKeyElement, KeySchemaElement rangeKeyElement) {
        if (hashKeyElement == null) {
            throw new RuntimeException("hashKeyElement is mandatory, and can not be null");
        }
        this.hashKeyElement = hashKeyElement;
        this.rangeKeyElement = rangeKeyElement;
    }

    public PrimaryKey(KeySchemaElement hashKeyElement) {
        this(hashKeyElement, null);
    }

    public KeySchemaElement hashKeyElement() {
        return this.hashKeyElement;
    }

    public KeySchemaElement rangeKeyElement() {
        return this.rangeKeyElement;
    }

    public static PrimaryKey fromScala(aws.dynamodb.PrimaryKey scalaSchema) {
        KeySchemaElement rangeKey = null;
        if (scalaSchema instanceof aws.dynamodb.CompositeKey) {
            aws.dynamodb.CompositeKey key = (aws.dynamodb.CompositeKey)scalaSchema;
            rangeKey = KeySchemaElement.fromScala(key.rangeKey());
        }
        return new PrimaryKey(KeySchemaElement.fromScala(scalaSchema.hashKey()), rangeKey);
    }

    public aws.dynamodb.PrimaryKey toScala() {
        if (rangeKeyElement == null) {
            return new aws.dynamodb.HashKey(hashKeyElement.toScala());
        }
        return new aws.dynamodb.CompositeKey(hashKeyElement.toScala(), rangeKeyElement.toScala());
    }

    @Override
    public int hashCode() {
        return toScala().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;

        if (obj instanceof PrimaryKey == false) return false;
        PrimaryKey other = (PrimaryKey)obj;
        return toScala().equals(other.toScala());
    }

}
