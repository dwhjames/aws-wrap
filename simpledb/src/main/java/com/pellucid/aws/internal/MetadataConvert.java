package com.pellucid.aws.internal;

import play.libs.F.Function;
import com.pellucid.aws.simpledb.SimpleDBMeta;

public class MetadataConvert implements Function<aws.simpledb.SimpleDBMeta, SimpleDBMeta> {

    @Override
    public SimpleDBMeta apply(aws.simpledb.SimpleDBMeta scalaMeta) throws Throwable {
        return new SimpleDBMeta(scalaMeta.requestId(), scalaMeta.boxUsage());
    }

}
