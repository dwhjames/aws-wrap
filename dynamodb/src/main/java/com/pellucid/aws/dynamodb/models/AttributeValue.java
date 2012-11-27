package com.pellucid.aws.dynamodb.models;

import java.nio.ByteBuffer;

import aws.dynamodb.DDBNumber;
import aws.dynamodb.DDBString;

public class AttributeValue {

    public enum Type {
        S, N, B, SS, NN, BB
    }

    private final aws.dynamodb.DDBAttribute scalaAttribute;

    private AttributeValue(aws.dynamodb.DDBAttribute scalaAttribute) {
        this.scalaAttribute = scalaAttribute;
    }

    public Type type() {
        if (this.scalaAttribute instanceof DDBString) return Type.S;
        if (this.scalaAttribute instanceof DDBNumber) return Type.N;
        
        throw new RuntimeException("Unkown type: " + this.scalaAttribute.toString());
    }

    public aws.dynamodb.DDBAttribute toScala() {
        return this.scalaAttribute;
    }

    public static AttributeValue fromScala(aws.dynamodb.DDBAttribute scalaAttribute) {
        return new AttributeValue(scalaAttribute);
    }

    public static AttributeValue createS(String value) {
        return new AttributeValue(new aws.dynamodb.DDBString(value));
    }

    public static AttributeValue createN(Double value) {
        return new AttributeValue(new aws.dynamodb.DDBNumber(value));
    }

    public static AttributeValue createB(ByteBuffer value) {
        return new AttributeValue(new aws.dynamodb.DDBBinary(value.array()));
    }

}
