package com.pellucid.aws.dynamodb.models;

public class KeyCondition {

    public enum ConditionType {
        EqualTo, LessThan, GreaterThan, BeginsWith, Between
    }

    private final ConditionType type;

    private final AttributeValue primary;

    private final AttributeValue secondary;

    private KeyCondition(ConditionType type, AttributeValue primary, AttributeValue secondary) {
        this.type = type;
        this.primary = primary;
        this.secondary = secondary;
    }

    public static KeyCondition equalTo(AttributeValue value) {
        return new KeyCondition(ConditionType.EqualTo, value, null);
    }

    public static KeyCondition lessThan(AttributeValue value) {
        return new KeyCondition(ConditionType.LessThan, value, null);
    }

    public static KeyCondition greaterThan(AttributeValue value) {
        return new KeyCondition(ConditionType.GreaterThan, value, null);
    }

    public static KeyCondition beginsWith(AttributeValue value) {
        return new KeyCondition(ConditionType.BeginsWith, value, null);
    }

    public static KeyCondition between(AttributeValue lowerBound, AttributeValue upperBound) {
        return new KeyCondition(ConditionType.BeginsWith, lowerBound, upperBound);
    }

    public ConditionType type() {
        return type;
    }

    public AttributeValue primary() {
        return primary;
    }

    public AttributeValue secondary() {
        return secondary;
    }

    public aws.dynamodb.KeyCondition toScala() {
        switch (this.type) {
        case EqualTo: return new aws.dynamodb.EqualTo(this.primary.toScala());
        }
        return null; // Never happens
    }

}
