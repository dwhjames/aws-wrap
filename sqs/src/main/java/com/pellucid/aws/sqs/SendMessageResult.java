package com.pellucid.aws.sqs;

public class SendMessageResult {

    private aws.sqs.SendMessageResult scalaSMR;
    
    private SendMessageResult(aws.sqs.SendMessageResult scalaSMR) {
        this.scalaSMR = scalaSMR;
    }
    
    public String messageId() {
        return this.scalaSMR.messageId();
    }
    
    public String md5() {
        return this.scalaSMR.md5();
    }
    
    public static SendMessageResult fromScala(aws.sqs.SendMessageResult scalaSMR) {
        return new SendMessageResult(scalaSMR);
    }
    
}
