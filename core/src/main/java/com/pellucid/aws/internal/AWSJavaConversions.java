package com.pellucid.aws.internal;

import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.concurrent.Future;
import akka.dispatch.*;

import com.pellucid.aws.results.Result;
import com.pellucid.aws.results.SuccessResult;
import com.pellucid.aws.results.SimpleResult;
import com.pellucid.aws.results.SimpleSuccess;
import com.pellucid.aws.results.SimpleError;
import com.pellucid.aws.results.AWSError;


@SuppressWarnings("unchecked")
public class AWSJavaConversions {

    public static <MS extends aws.core.Metadata, TS, MJ, TJ> Result<MJ, TJ> toJavaResult(aws.core.Result<MS, TS> scalaResult,
            final Mapper<MS, MJ> metadataConvert,
            final Mapper<TS, TJ> bodyConvert) {
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

    public static <MS extends aws.core.Metadata, TS, MJ, TJ> Future<Result<MJ, TJ>> toJavaResultFuture(
            Future<aws.core.Result<MS, TS>> scalaResult,
            final Mapper<MS, MJ> metadataConvert,
            final Mapper<TS, TJ> bodyConvert) {
        return scalaResult.map(new Mapper<aws.core.Result<MS, TS>, Result<MJ, TJ>>(){
            @Override public Result<MJ, TJ> apply(aws.core.Result<MS, TS> scalaResult) {
                return toJavaResult(scalaResult, metadataConvert, bodyConvert);
            }
        }, aws.core.AWS.defaultExecutionContext());
    }

    public static <MS extends aws.core.Metadata, TS, TJ> SimpleResult<TJ> toJavaSimpleResult(aws.core.Result<MS, TS> scalaResult,
            final Mapper<TS, TJ> bodyConvert) {
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

    public static <A, B> scala.collection.immutable.Map<A, B> toScalaMap(java.util.Map<A, B> m) {
        return JavaConverters.mapAsScalaMapConverter(m).asScala().toMap(
          Predef.<Tuple2<A, B>>conforms()
        );
    }

    public static <MS extends aws.core.Metadata, TS, TJ> Future<SimpleResult<TJ>> toJavaSimpleResult(Future<aws.core.Result<MS, TS>> scalaFuture,
            final Mapper<TS, TJ> bodyConvert) {
        return scalaFuture.map(new Mapper<aws.core.Result<MS, TS>, SimpleResult<TJ>>(){
            @Override public SimpleResult<TJ> apply(aws.core.Result<MS, TS> scalaResult) {
                return toJavaSimpleResult(scalaResult, bodyConvert);
            }
        }, aws.core.AWS.defaultExecutionContext());
    }

}

