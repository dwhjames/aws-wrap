package com.pellucid.aws.s3.models;

import java.util.Date;

import play.libs.Scala;

import com.pellucid.aws.s3.S3;
import com.pellucid.aws.s3.models.S3Object.StorageClass;

public class Version extends Container {

    private aws.s3.models.Version scalaVersion;

    private Version(aws.s3.models.Version scalaVersion) {
        this.scalaVersion = scalaVersion;
    }

    public static Version fromScala(aws.s3.models.Version scalaVersion) {
        return new Version(scalaVersion);
    }

    @Override protected aws.s3.models.Container scalaContainer() {
        return this.scalaVersion;
    }

}
