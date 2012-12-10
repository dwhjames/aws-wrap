package com.pellucid.aws.sqs;

import play.libs.Scala;
import scala.concurrent.Future;
import akka.dispatch.Mapper;

import com.pellucid.aws.internal.AWSJavaConversions;
import com.pellucid.aws.results.Result;

public class Queue {

    private final String url;

    public Queue(String url) {
        this.url = url;
    }

    public String url() {
        return url;
    }

    public Future<Result<SQSMeta, SendMessageResult>> sendMessage(String message) {
        return this.sendMessage(message, null);
    }

    public Future<Result<SQSMeta, SendMessageResult>> sendMessage(String message, Long delaySeconds) {
        return AWSJavaConversions.toJavaResultFuture(toScala().sendMessage(message, Scala.Option((Object)delaySeconds)),
                new SQS.MetadataConvert(),
                new Mapper<aws.sqs.SendMessageResult, SendMessageResult>() {
            @Override public SendMessageResult apply(aws.sqs.SendMessageResult result) {
                return SendMessageResult.fromScala(result);
            }
        });
    }

    public aws.sqs.SQS.Queue toScala() {
        return new aws.sqs.SQS.Queue(this.url);
    }

    public static Queue fromScala(aws.sqs.SQS.Queue scalaQueue) {
        return new Queue(scalaQueue.url());
    }

}
