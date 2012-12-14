package com.pellucid.aws.s3;

import akka.dispatch.Mapper;

public class S3 {

    public enum HTTPMethod {
        PUT, POST, DELETE, GET
    }

    static scala.Enumeration.Value toScalaMethod(HTTPMethod method) {
        switch (method) {
        case PUT: return aws.s3.S3.HTTPMethods$.MODULE$.PUT();
        case POST: return aws.s3.S3.HTTPMethods$.MODULE$.POST();
        case DELETE: return aws.s3.S3.HTTPMethods$.MODULE$.DELETE();
        case GET: return aws.s3.S3.HTTPMethods$.MODULE$.GET();
        }
        return null;
    }

    public static HTTPMethod fromScalaMethod(scala.Enumeration.Value scalaMethod) {
        if (scalaMethod == aws.s3.S3.HTTPMethods$.MODULE$.PUT()) return HTTPMethod.PUT;
        if (scalaMethod == aws.s3.S3.HTTPMethods$.MODULE$.POST()) return HTTPMethod.POST;
        if (scalaMethod == aws.s3.S3.HTTPMethods$.MODULE$.DELETE()) return HTTPMethod.DELETE;
        if (scalaMethod == aws.s3.S3.HTTPMethods$.MODULE$.GET()) return HTTPMethod.GET;
        return null;
    }

    static aws.s3.S3Region scalaRegion(S3Region region) {
        switch (region) {
        case US_EAST_1: return aws.s3.S3Region$.MODULE$.US_EAST_1();
        case US_WEST_1: return aws.s3.S3Region$.MODULE$.US_WEST_1();
        case US_WEST_2: return aws.s3.S3Region$.MODULE$.US_WEST_2();
        case EU_WEST_1: return aws.s3.S3Region$.MODULE$.EU_WEST_1();
        case SA_EAST_1: return aws.s3.S3Region$.MODULE$.SA_EAST_1();
        }
        return aws.s3.S3Region$.MODULE$.DEFAULT();
    }

    static class MetadataConvert extends Mapper<aws.s3.models.S3Metadata, S3Metadata> {
        @Override
        public S3Metadata apply(aws.s3.models.S3Metadata scalaMeta) {
            return S3Metadata.fromScala(scalaMeta);
        }
    }


}
