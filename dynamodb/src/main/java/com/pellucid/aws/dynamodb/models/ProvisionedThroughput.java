package com.pellucid.aws.dynamodb.models;

public class ProvisionedThroughput {

    private Long readCapacityUnits;
    private Long writeCapacityUnits;

    public ProvisionedThroughput(Long readCapacityUnits, Long writeCapacityUnits) {
        this.readCapacityUnits = readCapacityUnits;
        this.writeCapacityUnits = writeCapacityUnits;
    }

    public Long readCapacityUnits() {
        return this.readCapacityUnits;
    }

    public Long writeCapacityUnits() {
        return this.writeCapacityUnits;
    }
/*
    public aws.dynamodb.ProvisionedThroughput toScala() {
        return new aws.dynamodb.ProvisionedThroughput(readCapacityUnits, writeCapacityUnits);
    }
*/
}
