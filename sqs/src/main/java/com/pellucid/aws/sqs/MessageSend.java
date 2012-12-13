package com.pellucid.aws.sqs;

import play.libs.Scala;

public class MessageSend {

    private aws.sqs.MessageSend scalaMessage;

    public MessageSend(String id, String body) {
        this(id, body, null);
    }

    public MessageSend(String id, String body, Long delaySeconds) {
        this.scalaMessage = new aws.sqs.MessageSend(id, body, Scala.Option((Object)delaySeconds));
    }

    public aws.sqs.MessageSend toScala() {
        return this.scalaMessage;
    }
    
}

// case class MessageSend(id: String, body: String, delaySeconds: Option[Long] = None)
