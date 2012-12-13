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

    public Future<Result<SQSMeta, List<QueueAttributeValue>>> getAttributes(List<QueueAttribute> attributes) {
        return AWSJavaConversions.toJavaResultFuture(toScala().getAttributes(
                AWSJavaConversions.toSeq(Lists.map(attributes, new Mapper<QueueAttribute, aws.sqs.QueueAttribute>() {
                    @Override public aws.sqs.QueueAttribute apply(QueueAttribute attribute) {
                        return aws.sqs.QueueAttribute$.MODULE$.apply(attribute.toString());
                    }
                }))
                ),
                new SQS.MetadataConvert(),
                new Mapper<Seq<aws.sqs.QueueAttributeValue>, List<QueueAttributeValue>>() {
            @Override public List<QueueAttributeValue> apply(Seq<aws.sqs.QueueAttributeValue> result) {
                return Lists.map(JavaConversions.seqAsJavaList(result), new Mapper<aws.sqs.QueueAttributeValue, QueueAttributeValue>(){
                    @Override public QueueAttributeValue apply(aws.sqs.QueueAttributeValue sQueueAttributeValue) {
                        return QueueAttributeValue.fromScala(sQueueAttributeValue);
                    }
                });
            }
        });
    }

    public Future<Result<SQSMeta, Object>> setAttributes(List<QueueAttributeValue> attributes) {
        return SQS.convertEmptyResult(toScala().setAttributes(convertAttributeValueList(attributes)));
    }

    public Future<Result<SQSMeta, Object>> addPermission(String label, List<String> accountIds, List<ActionName> actionNames) {
        return SQS.convertEmptyResult(toScala().addPermission(
                label,
                AWSJavaConversions.toSeq(accountIds),
                AWSJavaConversions.toSeq(Lists.map(actionNames, new Mapper<ActionName, aws.sqs.ActionName>(){
                    @Override public aws.sqs.ActionName apply(ActionName actionName) {
                        return aws.sqs.ActionName$.MODULE$.apply(actionName.toString());
                    }
                }))
                ));
    }

    public Future<Result<SQSMeta, Object>> removePermission(String label) {
        return SQS.convertEmptyResult(toScala().removePermission(label));
    }

    public Future<Result<SQSMeta, Object>> deleteMessage(String receiptHandle) {
        return SQS.convertEmptyResult(toScala().deleteMessage(receiptHandle));
    }

    public Future<Result<SQSMeta, List<MessageResponse>>> sendMessageBatch(List<MessageSend> messages) {
        return AWSJavaConversions.toJavaResultFuture(toScala().sendMessageBatch(
                AWSJavaConversions.toSeq(Lists.map(messages, new Mapper<MessageSend, aws.sqs.MessageSend>(){
                    @Override public aws.sqs.MessageSend apply(MessageSend message) {
                        return message.toScala();
                    }
                }))),
                new SQS.MetadataConvert(),
                new Mapper<Seq<aws.sqs.MessageResponse>, List<MessageResponse>>() {
            @Override public List<MessageResponse> apply(Seq<aws.sqs.MessageResponse> result) {
                return Lists.map(JavaConversions.seqAsJavaList(result), new Mapper<aws.sqs.MessageResponse, MessageResponse>(){
                    @Override public MessageResponse apply(aws.sqs.MessageResponse sQueueAttributeValue) {
                        return MessageResponse.fromScala(sQueueAttributeValue);
                    }
                });
            }
        });
    }

    public Future<Result<SQSMeta, List<String>>> deleteMessageBatch(List<MessageDelete> messages) {
        return AWSJavaConversions.toJavaResultFuture(toScala().deleteMessageBatch(
                AWSJavaConversions.toSeq(Lists.map(messages, new Mapper<MessageDelete, aws.sqs.MessageDelete>(){
                    @Override public aws.sqs.MessageDelete apply(MessageDelete message) {
                        return message.toScala();
                    }
                }))),
                new SQS.MetadataConvert(),
                new Mapper<Seq<String>, List<String>>() {
            @Override public List<String> apply(Seq<String> result) {
                return JavaConversions.seqAsJavaList(result);
            }
        });
    }

    public Future<Result<SQSMeta, Object>> changeMessageVisibility(String receiptHandle, Long visibilityTimeout) {
        return SQS.convertEmptyResult(toScala().changeMessageVisibility(receiptHandle, visibilityTimeout));
    }

    public Future<Result<SQSMeta, List<String>>> changeMessageVisibilityBatch(List<MessageVisibility> messages) {
        return AWSJavaConversions.toJavaResultFuture(toScala().changeMessageVisibilityBatch(
                AWSJavaConversions.toSeq(Lists.map(messages, new Mapper<MessageVisibility, aws.sqs.MessageVisibility>(){
                    @Override public aws.sqs.MessageVisibility apply(MessageVisibility message) {
                        return message.toScala();
                    }
                }))),
                new SQS.MetadataConvert(),
                new Mapper<Seq<String>, List<String>>() {
            @Override public List<String> apply(Seq<String> result) {
                return JavaConversions.seqAsJavaList(result);
            }
        });
    }

    private Seq<aws.sqs.QueueAttributeValue> convertAttributeValueList(List<QueueAttributeValue> attributes) {
        return AWSJavaConversions.toSeq(Lists.map(attributes, new Mapper<QueueAttributeValue, aws.sqs.QueueAttributeValue>(){
            @Override public aws.sqs.QueueAttributeValue apply(QueueAttributeValue attributeValue) {
                return attributeValue.toScala();
            }
        }));
    }

    public aws.sqs.SQS.Queue toScala() {
        return new aws.sqs.SQS.Queue(this.url);
    }

    public static Queue fromScala(aws.sqs.SQS.Queue scalaQueue) {
        return new Queue(scalaQueue.url());
    }

}
