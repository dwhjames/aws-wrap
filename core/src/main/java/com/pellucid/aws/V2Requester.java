package com.pellucid.aws;

import java.util.Map;

import aws.core.parsers.Parser;
import com.pellucid.aws.results.Result;

import scala.concurrent.Future;


public class V2Requester<M> {

    private aws.core.AWSRegion region;

    public V2Requester(aws.core.AWSRegion region) {
        this.region = region;
    }

    protected <T> Future<Result<M, T>> get(String resource, Map<String, String> parameters, Parser<T> parser) {
        return null;
    }

}
