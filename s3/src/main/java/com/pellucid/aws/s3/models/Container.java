package com.pellucid.aws.s3.models;

import java.util.Date;

import play.libs.Scala;

import com.pellucid.aws.s3.S3;
import com.pellucid.aws.s3.models.S3Object.StorageClass;

public abstract class Container {

    protected abstract aws.s3.models.Container scalaContainer();

    public String id() {
        return Scala.orNull(scalaContainer().id());
    }

    public String key() {
        return scalaContainer().key();
    }

    public Boolean isLatest() {
        return scalaContainer().isLatest();
    }

    public Date lastModified() {
        return scalaContainer().lastModified();
    }

    public String etag() {
        return scalaContainer().etag();
    }

    public Long size() {
        return (Long)Scala.orNull(scalaContainer().size());
    }

    public StorageClass storageClass() {
        return S3.storageClassFromScala(Scala.orNull(scalaContainer().storageClass()));
    }

    public Owner owner() {
        return Owner.fromScala(scalaContainer().owner());
    }

}
