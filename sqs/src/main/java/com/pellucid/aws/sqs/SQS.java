package com.pellucid.aws.sqs;

import java.util.List;

import scala.collection.JavaConversions;
import scala.concurrent.Future;
import akka.dispatch.Mapper;

import com.pellucid.aws.internal.AWSJavaConversions;
import com.pellucid.aws.results.Result;
import com.pellucid.aws.utils.Lists;

public class SQS {

    private final aws.sqs.SQSRegion scalaRegion;

    public SQS(SQSRegion region) {
        this.scalaRegion = scalaRegion(region);
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
    /*
      def listQueues(queueNamePrefix: String = "")(implicit region: SQSRegion): Future[Result[SQSMeta, Seq[Queue]]] = {
        val params = Seq(Action("ListQueues")) ++ QueueNamePrefix(queueNamePrefix)
        get[Seq[Queue]](params: _*)
      }

      def deleteQueue(queueURL: String): Future[EmptyResult[SQSMeta]] = {
        val params = Seq(Action("DeleteQueue"))
        get[Unit](queueURL, params: _*)
      }

      def getQueue(name: String, queueOwnerAWSAccountId: Option[String] = None)(implicit region: SQSRegion): Future[Result[SQSMeta, Queue]] = {
        val params = Seq(Action("GetQueueUrl"), QueueName(name)) ++ QueueOwnerAWSAccountId(queueOwnerAWSAccountId)
        get[Queue](params: _*)
      }
     */

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

    private static class QueueConvert extends Mapper<aws.sqs.SQS.Queue, Queue> {
        @Override
        public Queue apply(aws.sqs.SQS.Queue scalaQueue) {
            return Queue.fromScala(scalaQueue);
        }
    }

    private static class MetadataConvert extends Mapper<aws.sqs.SQSMeta, SQSMeta> {
        @Override
        public SQSMeta apply(aws.sqs.SQSMeta scalaMeta) {
            return new SQSMeta(scalaMeta.requestId());
        }
    }

}
