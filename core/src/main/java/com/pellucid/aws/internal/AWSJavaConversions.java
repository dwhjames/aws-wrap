package com.pellucid.aws.internal;

import java.util.HashMap;
import java.util.Map;

import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.concurrent.Future;
import scala.concurrent.ExecutionContext;
import akka.dispatch.Mapper;

import com.pellucid.aws.results.AWSError;
import com.pellucid.aws.results.Result;
import com.pellucid.aws.results.SimpleError;
import com.pellucid.aws.results.SimpleResult;
import com.pellucid.aws.results.SimpleSuccess;
import com.pellucid.aws.results.SuccessResult;

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
            final Mapper<TS, TJ> bodyConvert, final ExecutionContext executionContext) {
        return scalaResult.map(new Mapper<aws.core.Result<MS, TS>, Result<MJ, TJ>>(){
            @Override public Result<MJ, TJ> apply(aws.core.Result<MS, TS> scalaResult) {
                return toJavaResult(scalaResult, metadataConvert, bodyConvert);
            }
        }, executionContext);
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

    public static <A, B> scala.collection.immutable.Map<String, B> toScalaMap(Map<String, A> javaMap, Mapper<A, B> convert) {
        Map<String, B> result = new HashMap<String, B>();
        for (String key: javaMap.keySet()) {
            result.put(key, convert.apply(javaMap.get(key)));
        }
        return toScalaMap(result);
    }

    public static <A> scala.collection.immutable.Set<A> toScalaSet(java.util.Set<A> m) {
        return JavaConverters.asScalaSetConverter(m).asScala().toSet();
    }

    public static <A> scala.collection.Seq<A> toSeq(java.util.List<A> l) {
        return JavaConversions.iterableAsScalaIterable(l).toSeq();
    }

    public static <A, B> Map<String, B> toJavaMap(scala.collection.Map<String, A> sMap, Mapper<A, B> convert) {
        Map<String, B> result = new HashMap<String, B>();
        for (String key: JavaConversions.asJavaIterable(sMap.keys())) {
            result.put(key, convert.apply(sMap.apply(key)));
        }
        return result;
    }

    public static <A, B> Map<A, B> toJavaMap(Seq<Tuple2<A, B>> seqOfPairs) {
        Map<A, B> result = new HashMap<A, B>();
        for (Tuple2<A, B> pair: JavaConversions.asJavaIterable(seqOfPairs)) {
            result.put(pair._1(), pair._2());
        }
        return result;
    }

    public static <MS extends aws.core.Metadata, TS, TJ> Future<SimpleResult<TJ>> toJavaSimpleResult(Future<aws.core.Result<MS, TS>> scalaFuture,
            final Mapper<TS, TJ> bodyConvert, final ExecutionContext executionContext) {
        return scalaFuture.map(new Mapper<aws.core.Result<MS, TS>, SimpleResult<TJ>>(){
            @Override public SimpleResult<TJ> apply(aws.core.Result<MS, TS> scalaResult) {
                return toJavaSimpleResult(scalaResult, bodyConvert);
            }
        }, executionContext);
    }

}

