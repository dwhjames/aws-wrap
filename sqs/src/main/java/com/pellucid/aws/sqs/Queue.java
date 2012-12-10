package com.pellucid.aws.sqs;

public class Queue {

    private final String url;

    public Queue(String url) {
        this.url = url;
    }

    public String url() {
        return url;
    }

    public aws.sqs.SQS.Queue toScala() {
        return new aws.sqs.SQS.Queue(this.url);
    }

    public static Queue fromScala(aws.sqs.SQS.Queue scalaQueue) {
        return new Queue(scalaQueue.url());
    }

}
