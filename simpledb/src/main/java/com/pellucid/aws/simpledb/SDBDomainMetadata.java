package com.pellucid.aws.simpledb;

import java.util.Date;

public class SDBDomainMetadata {

    private final Date timestamp;
    private final Long itemCount;
    private final Long attributeValueCount;
    private final Long attributeNameCount;
    private final Long itemNamesSizeBytes;
    private final Long attributeValuesSizeBytes;
    private final Long attributeNamesSizeBytes;

    public SDBDomainMetadata(
            Date timestamp,
            Long itemCount,
            Long attributeValueCount,
            Long attributeNameCount,
            Long itemNamesSizeBytes,
            Long attributeValuesSizeBytes,
            Long attributeNamesSizeBytes) {
        this.timestamp = timestamp;
        this.itemCount = itemCount;
        this.attributeValueCount = attributeValueCount;
        this.attributeNameCount = attributeNameCount;
        this.itemNamesSizeBytes = itemNamesSizeBytes;
        this.attributeValuesSizeBytes = attributeValuesSizeBytes;
        this.attributeNamesSizeBytes = attributeNamesSizeBytes;
    }


    public SDBDomainMetadata(Date timestamp) {
        this.timestamp = timestamp;
        this.itemCount = 0L;
        this.attributeValueCount = 0L;
        this.attributeNameCount = 0L;
        this.itemNamesSizeBytes = 0L;
        this.attributeValuesSizeBytes = 0L;
        this.attributeNamesSizeBytes = 0L;
    }

    public Date timestamp() {
        return this.timestamp;
    }

    public Long itemCount() {
        return this.itemCount;
    }

    public Long attributeValueCount() {
        return this.attributeValueCount;
    }

    public Long attributeNameCount() {
        return this.attributeNameCount;
    }

    public Long itemNamesSizeBytes() {
        return this.itemNamesSizeBytes;
    }

    public Long attributeValuesSizeBytes() {
        return this.attributeValuesSizeBytes;
    }

    public Long attributeNamesSizeBytes() {
        return this.attributeNamesSizeBytes;
    }

    static SDBDomainMetadata fromScalaMetadata(aws.simpledb.SDBDomainMetadata scalaMeta) {
        return new SDBDomainMetadata(
                scalaMeta.timestamp(),
                scalaMeta.itemCount(),
                scalaMeta.attributeValueCount(),
                scalaMeta.attributeNameCount(),
                scalaMeta.itemNamesSizeBytes(),
                scalaMeta.attributeValuesSizeBytes(),
                scalaMeta.attributeNamesSizeBytes());
    }

}
