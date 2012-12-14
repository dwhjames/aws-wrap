package com.pellucid.aws.s3;

import play.libs.Scala;

public class S3Metadata {

    private String requestId;

    private String id2;

    private String versionId;

    private boolean deleteMarker;

    public S3Metadata(String requestId, String id2) {
        this(requestId, id2, null, false);
    }

    public S3Metadata(String requestId, String id2, boolean deleteMarker) {
        this(requestId, id2, null, deleteMarker);
    }

    public S3Metadata(String requestId, String id2, String versionId) {
        this(requestId, id2, versionId, false);
    }

    public S3Metadata(String requestId, String id2, String versionId, boolean deleteMarker) {
        this.requestId = requestId;
        this.id2 = id2;
        this.versionId = versionId;
        this.deleteMarker = deleteMarker;
    }

    public String requestId() {
        return requestId;
    }

    public String id2() {
        return id2;
    }

    public String versionId() {
        return versionId;
    }

    public boolean deleteMarker() {
        return deleteMarker;
    }

    public static S3Metadata fromScala(aws.s3.models.S3Metadata scalaMeta) {
        return new S3Metadata(scalaMeta.requestId(), scalaMeta.id2(), Scala.orNull(scalaMeta.versionId()), scalaMeta.deleteMarker());
    }

}
