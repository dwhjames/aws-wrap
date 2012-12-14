package com.pellucid.aws.s3.models;

import java.util.List;

import play.libs.Scala;
import scala.collection.JavaConversions;
import akka.dispatch.Mapper;

import com.pellucid.aws.s3.S3;
import com.pellucid.aws.s3.S3.HTTPMethod;
import com.pellucid.aws.utils.Lists;

public class CORSRule {

    private aws.s3.models.CORSRule scalaRules;

    private CORSRule(aws.s3.models.CORSRule scalaRules) {
        this.scalaRules = scalaRules;
    }

    public List<String> origins() {
        return JavaConversions.seqAsJavaList(scalaRules.origins());
    }

    public List<HTTPMethod> methods() {
        return Lists.map(JavaConversions.seqAsJavaList(scalaRules.methods()), new Mapper<scala.Enumeration.Value, HTTPMethod>(){
            @Override public HTTPMethod apply(scala.Enumeration.Value scalaMethod) {
                return S3.fromScalaMethod(scalaMethod);
            }
        });
    }

    public List<String> headers() {
        return JavaConversions.seqAsJavaList(scalaRules.headers());
    }

    public Long maxAge() {
        return (Long)Scala.orNull(scalaRules.maxAge());
    }

    public List<String> exposeHeaders() {
        return JavaConversions.seqAsJavaList(scalaRules.exposeHeaders());
    }

    public static CORSRule fromScala(aws.s3.models.CORSRule scalaRules) {
        return new CORSRule(scalaRules);
    }

}
