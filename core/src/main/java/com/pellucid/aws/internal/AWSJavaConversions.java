package com.pellucid.aws.internal;

import scala.concurrent.Future;
import play.libs.F;

import com.pellucid.aws.results.Result;
import com.pellucid.aws.results.SuccessResult;
import com.pellucid.aws.results.SimpleResult;
import com.pellucid.aws.results.SimpleSuccess;
import com.pellucid.aws.results.SimpleError;
import com.pellucid.aws.results.AWSError;


public class AWSJavaConversions {

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

    public static <MS extends aws.core.Metadata, TS, TJ> SimpleResult<TJ> toJavaSimpleResult(aws.core.Result<MS, TS> scalaResult,
            final F.Function<TS, TJ> bodyConvert) {
        try {
            if (scalaResult instanceof aws.core.AWSError) {
                aws.core.AWSError err = (aws.core.AWSError)scalaResult;
                return ((SimpleResult<TJ>)new SimpleError(err.code(), err.message()));
            }
            return new SimpleSuccess(bodyConvert.apply(scalaResult.body()));
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

    public static <MS extends aws.core.Metadata, TS, TJ> F.Promise<SimpleResult<TJ>> toSimpleResultPromise(Future<aws.core.Result<MS, TS>> scalaFuture,
            final F.Function<TS, TJ> bodyConvert) {
        return new F.Promise(scalaFuture).map(new F.Function<aws.core.Result<MS, TS>, SimpleResult<TJ>>(){
            @Override public SimpleResult<TJ> apply(aws.core.Result<MS, TS> scalaResult) throws Throwable {
                return toJavaSimpleResult(scalaResult, bodyConvert);
            }
        });
    }


}

