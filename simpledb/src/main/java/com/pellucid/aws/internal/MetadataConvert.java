package com.pellucid.aws.internal;

import akka.dispatch.Mapper;
import com.pellucid.aws.simpledb.SimpleDBMeta;

public class MetadataConvert extends Mapper<aws.simpledb.SimpleDBMeta, SimpleDBMeta> {

    @Override
    public SimpleDBMeta apply(aws.simpledb.SimpleDBMeta scalaMeta) {
        return new SimpleDBMeta(scalaMeta.requestId(), scalaMeta.boxUsage());
    }

}
