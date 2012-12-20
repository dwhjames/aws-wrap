package com.pellucid.aws.s3.models;

import java.util.List;

import play.libs.Scala;
import scala.collection.JavaConversions;
import akka.dispatch.Mapper;

import com.pellucid.aws.utils.Lists;

public class BatchDeletion {

    public static class DeletionSuccess {
        private aws.s3.models.BatchDeletion.DeletionSuccess scalaObject;
        public DeletionSuccess(aws.s3.models.BatchDeletion.DeletionSuccess scalaObject) {
            this.scalaObject = scalaObject;
        }
        public String key() {
            return scalaObject.key();
        }
        public String versionId() {
            return Scala.orNull(scalaObject.versionId());
        }
        public String deleteMarker() {
            return Scala.orNull(scalaObject.deleteMarker());
        }
        public String deleteMarkerVersionId() {
            return Scala.orNull(scalaObject.deleteMarkerVersionId());
        }
    }

    public static class DeletionFailure {
        private String key;
        private String code;
        private String message;
        public DeletionFailure(String key, String code, String message) {
            this.key = key;
            this.code = code;
            this.message = message;
        }
        public DeletionFailure(aws.s3.models.BatchDeletion.DeletionFailure scalaDF) {
            this(scalaDF.key(), scalaDF.code(), scalaDF.message());
        }
        public String key() {
            return this.key;
        }
        public String code() {
            return this.code;
        }
        public String message() {
            return this.message;
        }
    }

    private aws.s3.models.BatchDeletion scalaDeletion;

    public BatchDeletion(aws.s3.models.BatchDeletion scalaDeletion) {
        this.scalaDeletion = scalaDeletion;
    }

    public List<DeletionSuccess> successes() {
        return Lists.map(JavaConversions.seqAsJavaList(scalaDeletion.successes()),
                new Mapper<aws.s3.models.BatchDeletion.DeletionSuccess, DeletionSuccess>() {
            @Override public DeletionSuccess apply(aws.s3.models.BatchDeletion.DeletionSuccess scalaSuccess) {
                return new DeletionSuccess(scalaSuccess);
            }
        });
    }

    public List<DeletionFailure> failures() {
        return Lists.map(JavaConversions.seqAsJavaList(scalaDeletion.failures()),
                new Mapper<aws.s3.models.BatchDeletion.DeletionFailure, DeletionFailure>() {
            @Override public DeletionFailure apply(aws.s3.models.BatchDeletion.DeletionFailure scalaFailure) {
                return new DeletionFailure(scalaFailure);
            }
        });
    }

    public static BatchDeletion fromScala(aws.s3.models.BatchDeletion scalaDeletion) {
        return new BatchDeletion(scalaDeletion);
    }

}

/*
case class DeletionSuccess(
key: String,
versionId: Option[String],
deleteMarker: Option[String],
deleteMarkerVersionId: Option[String])

case class DeletionFailure(key: String, code: String, message: String)
 */