package com.pellucid.aws.dynamodb.models;

import java.util.Date;

import play.libs.Scala;

public class TableDescription {
    private String name;
    private TableStatus status;
    private Date creationDateTime;
    private PrimaryKey keySchema;
    private ProvisionedThroughput provisionedThroughput;
    private Long size;

    public TableDescription(
            String name,
            TableStatus status,
            Date creationDateTime,
            PrimaryKey keySchema,
            ProvisionedThroughput provisionedThroughput,
            Long size) {
        this.name = name;
        this.status = status;
        this.creationDateTime = creationDateTime;
        this.keySchema = keySchema;
        this.provisionedThroughput = provisionedThroughput;
        this.size = size;
    }

    public String name() {
        return name;
    }

    public TableStatus status() {
        return status;
    }

    public Date creationDateTime() {
        return creationDateTime;
    }

    public PrimaryKey keySchema() {
        return keySchema;
    }

    public ProvisionedThroughput provisionedThroughput() {
        return this.provisionedThroughput;
    }

    public Long size() {
        return this.size;
    }

    public aws.dynamodb.TableDescription toScala() {
        return new aws.dynamodb.TableDescription(
                name,
                aws.dynamodb.Status$.MODULE$.apply(status.toString()),
                creationDateTime,
                keySchema.toScala(),
                provisionedThroughput.toScala(),
                Scala.Option((Object)size));
    }

    public static TableDescription fromScala(aws.dynamodb.TableDescription tableDesc) {
        return new TableDescription(
            tableDesc.name(),
            TableStatus.valueOf(tableDesc.status().toString()),
            tableDesc.creationDateTime(),
            PrimaryKey.fromScala(tableDesc.keySchema()),
            ProvisionedThroughput.fromScala(tableDesc.provisionedThroughput()),
            (Long)Scala.orNull(tableDesc.size())
        );
    }
}
