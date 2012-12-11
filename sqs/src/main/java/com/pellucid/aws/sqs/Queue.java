package com.pellucid.aws.sqs;

import java.util.List;

import play.libs.Scala;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.concurrent.Future;
import akka.dispatch.Mapper;

import com.pellucid.aws.internal.AWSJavaConversions;
import com.pellucid.aws.results.Result;
import com.pellucid.aws.utils.Lists;

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

    public Future<Result<SQSMeta, List<MessageReceive>>> receiveMessage(
            List<MessageAttribute> attributes,
            Long maxNumber,
            Long visibilityTimeout,
            Long waitTimeSeconds) {
        return AWSJavaConversions.toJavaResultFuture(toScala().receiveMessage(
                SQS.convertMessageAttributes(attributes),
                Scala.Option((Object)maxNumber),
                Scala.Option((Object)visibilityTimeout),
                Scala.Option((Object)waitTimeSeconds)
                ),
                new SQS.MetadataConvert(),
                new Mapper<Seq<aws.sqs.MessageReceive>, List<MessageReceive>>() {
            @Override public List<MessageReceive> apply(Seq<aws.sqs.MessageReceive> result) {
                return Lists.map(JavaConversions.seqAsJavaList(result), new Mapper<aws.sqs.MessageReceive, MessageReceive>(){
                    @Override public MessageReceive apply(aws.sqs.MessageReceive sMessageReceive) {
                        return MessageReceive.fromScala(sMessageReceive);
                    }
                });
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
