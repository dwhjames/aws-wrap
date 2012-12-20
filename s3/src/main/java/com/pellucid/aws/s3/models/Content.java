package com.pellucid.aws.s3.models;

import java.util.Date;
import play.libs.Scala;

public class Content {

    private aws.s3.models.Content scalaContent;

    private Content(aws.s3.models.Content scalaContent) {
        this.scalaContent = scalaContent;
    }

    public String id() {
        return Scala.orNull(this.scalaContent.id());
    }

    public String key() {
        return this.scalaContent.key();
    }

    public Boolean isLatest() {
        return this.scalaContent.isLatest();
    }

    public Date lastModified() {
        return this.scalaContent.lastModified();
    }

    public String etag() {
        return this.scalaContent.etag();
    }
    
    public Long size() {
        return (Long)Scala.orNull(this.scalaContent.size());
    }
    /*
    public StorageClass storageClass() {
        return Scala.orNull(this.scalaContent.storageClass());
    }
    
    public Owner owner() {
        return this.scalaContent.owner();
    }
    */
    public static Content fromScala(aws.s3.models.Content scalaContent) {
        return new Content(scalaContent);
    }

}
