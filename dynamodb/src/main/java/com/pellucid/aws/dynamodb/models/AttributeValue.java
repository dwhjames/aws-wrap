package com.pellucid.aws.dynamodb.models;

import java.nio.ByteBuffer;
import java.util.Set;

import scala.collection.JavaConversions;

import aws.dynamodb.DDBNumber;
import aws.dynamodb.DDBString;
import aws.dynamodb.DDBBinary;

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

    public String getS() {
        if (this.scalaAttribute instanceof DDBString) return ((DDBString)this.scalaAttribute).value();
        throw new RuntimeException("Not a String type");
    }

    public Double getN() {
        if (this.scalaAttribute instanceof DDBNumber) return ((DDBNumber)this.scalaAttribute).value();
        throw new RuntimeException("Not a Number type");
    }

    public ByteBuffer getB() {
        if (this.scalaAttribute instanceof DDBBinary) return ByteBuffer.wrap(((DDBBinary)this.scalaAttribute).value());
        throw new RuntimeException("Not a Binary type");
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

    public static AttributeValue createN(Long value) {
        return createN(new Double(value));
    }

    public static AttributeValue createN(Integer value) {
        return createN(new Double(value));
    }

    public static AttributeValue createB(ByteBuffer value) {
        return new AttributeValue(new aws.dynamodb.DDBBinary(value.array()));
    }

    /* public static AttributeValue createSS(Set<String> value) {
        return new AttributeValue(new aws.dynamodb.DDBStringSet(JavaConversions.asScalaSet(value).toSet()));
    }*/

}
