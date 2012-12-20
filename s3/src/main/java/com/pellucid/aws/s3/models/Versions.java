package com.pellucid.aws.s3.models;

import java.util.List;

import play.libs.Scala;
import scala.collection.JavaConversions;
import akka.dispatch.Mapper;

import com.pellucid.aws.utils.Lists;

public class Versions {

    private aws.s3.models.Versions scalaVersions;

    private Versions(aws.s3.models.Versions scalaVersions) {
        this.scalaVersions = scalaVersions;
    }

    public String name() {
        return this.scalaVersions.name();
    }

    public String prefix() {
        return Scala.orNull(this.scalaVersions.prefix());
    }

    public String key() {
        return Scala.orNull(this.scalaVersions.key());
    }

    public Long maxKeys() {
        return (Long)this.scalaVersions.maxKeys();
    }

    public Boolean isTruncated() {
        return this.scalaVersions.isTruncated();
    }

    public List<Version> versions() {
        return Lists.map(JavaConversions.seqAsJavaList(scalaVersions.versions()), new Mapper<aws.s3.models.Version, Version>(){
            @Override public Version apply(aws.s3.models.Version scalaVersion) {
                return Version.fromScala(scalaVersion);
            }
        });
    }

    public String versionId() {
        return Scala.orNull(this.scalaVersions.versionId());
    }

    public static Versions fromScala(aws.s3.models.Versions scalaVersions) {
        return new Versions(scalaVersions);
    }

}

/*
 * case class Versions(
  name: String,
  prefix: Option[String],
  key: Option[String],
  maxKeys: Long,
  isTruncated: Boolean,
  versions: Seq[Version],
  deleteMarkers: Seq[DeleteMarker],
  versionId: Option[String])
 */
