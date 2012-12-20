package com.pellucid.aws.s3.models;

import java.util.List;

import play.libs.Scala;
import scala.collection.JavaConversions;
import akka.dispatch.Mapper;

import com.pellucid.aws.utils.Lists;

public class S3Object {

    private aws.s3.models.S3Object scalaObject;

    private S3Object(aws.s3.models.S3Object scalaObject) {
        this.scalaObject = scalaObject;
    }

    public String name() {
        return scalaObject.name();
    }

    public String prefix() {
        return Scala.orNull(scalaObject.prefix());
    }

    public String marker() {
        return Scala.orNull(scalaObject.marker());
    }

    public Long maxKeys() {
        return scalaObject.maxKeys();
    }

    public Boolean isTruncated() {
        return scalaObject.isTruncated();
    }

    public List<Content> contents() {
        return Lists.map(JavaConversions.seqAsJavaList(scalaObject.contents()), new Mapper<aws.s3.models.Content, Content>(){
            @Override public Content apply(aws.s3.models.Content scalaContent) {
                return Content.fromScala(scalaContent);
            }
        });
    }

    public static S3Object fromScala(aws.s3.models.S3Object scalaObject) {
        return new S3Object(scalaObject);
    }

}
