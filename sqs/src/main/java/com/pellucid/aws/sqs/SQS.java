package com.pellucid.aws.sqs;

import java.util.ArrayList;
import java.util.List;

import play.libs.Scala;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.concurrent.Future;
import scala.runtime.BoxedUnit;
import akka.dispatch.Mapper;

import com.pellucid.aws.internal.AWSJavaConversions;
import com.pellucid.aws.results.Result;
import com.pellucid.aws.utils.Lists;

public class SQS {

    private final aws.sqs.SQSRegion scalaRegion;

    public SQS(SQSRegion region) {
        this.scalaRegion = scalaRegion(region);
    }

    public Future<Result<SQSMeta, Queue>> createQueue(String name) {
        return createQueue(name, new ArrayList<QueueAttributeValue>());
    }

    public Future<Result<SQSMeta, Queue>> createQueue(String name, List<QueueAttributeValue> attributes) {
        return convertQueueResult(aws.sqs.SQS.createQueue(name,
                JavaConversions.asScalaIterable(Lists.map(attributes, new Mapper<QueueAttributeValue, aws.sqs.CreateAttributeValue>(){
                    @Override public aws.sqs.CreateAttributeValue apply(QueueAttributeValue attr) {
                        return (aws.sqs.CreateAttributeValue)attr.toScala();
                    }
                })).toSeq(),
                scalaRegion
                ));
    }

    public Future<Result<SQSMeta, List<Queue>>> listQueues() {
        return listQueues("");
    }

    public Future<Result<SQSMeta, List<Queue>>> listQueues(String queueNamePrefix) {
        return AWSJavaConversions.toJavaResultFuture(aws.sqs.SQS.listQueues(queueNamePrefix, scalaRegion),
                new MetadataConvert(),
                new Mapper<Seq<aws.sqs.SQS.Queue>, List<Queue>>() {
            @Override public List<Queue> apply(Seq<aws.sqs.SQS.Queue> queueSeq) {
                return Lists.map(JavaConversions.seqAsJavaList(queueSeq), new QueueConvert());
            }
        });
    }

    public Future<Result<SQSMeta, Object>> deleteQueue(String queueURL) {
        return convertEmptyResult(aws.sqs.SQS.deleteQueue(queueURL));
    }

    public Future<Result<SQSMeta, Queue>> getQueue(String name) {
        return getQueue(name, null);
    }

    public Future<Result<SQSMeta, Queue>> getQueue(String name, String queueOwnerAWSAccountId) {
        return convertQueueResult(aws.sqs.SQS.getQueue(name, Scala.Option(queueOwnerAWSAccountId), scalaRegion));
    }

    private static aws.sqs.SQSRegion scalaRegion(SQSRegion region) {
        switch (region) {
        case US_EAST_1: return aws.sqs.SQSRegion$.MODULE$.US_EAST_1();
        case US_WEST_1: return aws.sqs.SQSRegion$.MODULE$.US_WEST_1();
        case US_WEST_2: return aws.sqs.SQSRegion$.MODULE$.US_WEST_2();
        case EU_WEST_1: return aws.sqs.SQSRegion$.MODULE$.EU_WEST_1();
        case ASIA_SOUTHEAST_1: return aws.sqs.SQSRegion$.MODULE$.ASIA_SOUTHEAST_1();
        case ASIA_NORTHEAST_1: return aws.sqs.SQSRegion$.MODULE$.ASIA_NORTHEAST_1();
        case SA_EAST_1: return aws.sqs.SQSRegion$.MODULE$.SA_EAST_1();
        }
        return aws.sqs.SQSRegion$.MODULE$.DEFAULT();
    }

    private static Future<Result<SQSMeta, Queue>> convertQueueResult(Future<aws.core.Result<aws.sqs.SQSMeta, aws.sqs.SQS.Queue>> scalaResult) {
        return AWSJavaConversions.toJavaResultFuture(scalaResult, new MetadataConvert(), new QueueConvert());
    }

    private static Future<Result<SQSMeta, Object>> convertEmptyResult(Future<aws.core.Result<aws.sqs.SQSMeta, BoxedUnit>> scalaResult) {
        return AWSJavaConversions.toJavaResultFuture(scalaResult, new MetadataConvert(), new Mapper<BoxedUnit, Object>() {
            @Override public Object apply(BoxedUnit unit) {
                return null;
            }
        });
    }

    private static class QueueConvert extends Mapper<aws.sqs.SQS.Queue, Queue> {
        @Override
        public Queue apply(aws.sqs.SQS.Queue scalaQueue) {
            return Queue.fromScala(scalaQueue);
        }
    }

    static class MetadataConvert extends Mapper<aws.sqs.SQSMeta, SQSMeta> {
        @Override
        public SQSMeta apply(aws.sqs.SQSMeta scalaMeta) {
            return new SQSMeta(scalaMeta.requestId());
        }
    }

    static Seq<aws.sqs.MessageAttribute> convertMessageAttributes(List<MessageAttribute> attributes) {
        return AWSJavaConversions.toSeq(Lists.map(attributes, new MessageAttributeConvert()));
    }

    static class MessageAttributeConvert extends Mapper<MessageAttribute, aws.sqs.MessageAttribute> {
        @Override public aws.sqs.MessageAttribute apply(MessageAttribute jAttributes) {
            return aws.sqs.MessageAttribute$.MODULE$.apply(jAttributes.toString());
        }
    }


}
