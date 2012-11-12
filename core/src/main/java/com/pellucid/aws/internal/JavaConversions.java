package com.pellucid.aws.internal;

import scala.concurrent.Future;
import play.libs.F;

import com.pellucid.aws.results.Result;
import com.pellucid.aws.results.SuccessResult;
import com.pellucid.aws.results.AWSError;


public class JavaConversions {

    public static <MS extends aws.core.Metadata, TS, MJ, TJ> Result<MJ, TJ> toJavaResult(aws.core.Result<MS, TS> scalaResult,
            final F.Function<MS, MJ> metadataConvert,
            final F.Function<TS, TJ> bodyConvert) {
        try {
        MJ meta = metadataConvert.apply(scalaResult.metadata());
        if (scalaResult instanceof aws.core.AWSError) {
            aws.core.AWSError err = (aws.core.AWSError)scalaResult;
            return new AWSError(meta, err.code(), err.message());
        }
        return new SuccessResult(meta, bodyConvert.apply(scalaResult.body()));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public static <MS extends aws.core.Metadata, TS, MJ, TJ> F.Promise<Result<MJ, TJ>> toResultPromise(Future<aws.core.Result<MS, TS>> scalaFuture,
            final F.Function<MS, MJ> metadataConvert,
            final F.Function<TS, TJ> bodyConvert) {
        return new F.Promise(scalaFuture).map(new F.Function<aws.core.Result<MS, TS>, Result<MJ, TJ>>(){
            @Override public Result<MJ, TJ> apply(aws.core.Result<MS, TS> scalaResult) throws Throwable {
                return toJavaResult(scalaResult, metadataConvert, bodyConvert);
            }
        });
    }


}

