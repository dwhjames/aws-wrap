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

    public static PrimaryKey fromScala(aws.dynamodb.models.PrimaryKey scalaSchema) {
        KeySchemaElement rangeKey = null;
        if (scalaSchema instanceof aws.dynamodb.models.CompositeKey) {
            aws.dynamodb.models.CompositeKey key = (aws.dynamodb.models.CompositeKey)scalaSchema;
            rangeKey = KeySchemaElement.fromScala(key.rangeKey());
        }
        return new PrimaryKey(KeySchemaElement.fromScala(scalaSchema.hashKey()), rangeKey);
    }

    public aws.dynamodb.models.PrimaryKey toScala() {
        if (rangeKeyElement == null) {
            return new aws.dynamodb.models.CompositeKey(hashKeyElement.toScala(), rangeKeyElement.toScala());
        }
        return new aws.dynamodb.models.HashKey(hashKeyElement.toScala());
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
