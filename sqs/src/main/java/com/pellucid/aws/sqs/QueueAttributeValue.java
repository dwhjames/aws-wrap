package com.pellucid.aws.sqs;

import org.codehaus.jackson.JsonNode;

public class QueueAttributeValue {

    private String attribute;

    private String value;

    public QueueAttributeValue(String attribute, String value) {
        this.attribute = attribute;
        this.value = value;
    }

    public aws.sqs.QueueAttributeValue toScala() {
        return aws.sqs.QueueAttributeValue$.MODULE$.apply(attribute, value);
    }

    public static QueueAttributeValue approximateNumberOfMessages(Long value) {
        return new QueueAttributeValue("ApproximateNumberOfMessages", value.toString());
    }
    public static QueueAttributeValue  approximateNumberOfMessagesDelayed(Long value) {
        return new QueueAttributeValue("ApproximateNumberOfMessagesDelayed", value.toString());
    }
    public static QueueAttributeValue approximateNumberOfMessagesNotVisible(Long value) {
        return new QueueAttributeValue("ApproximateNumberOfMessagesNotVisible", value.toString());
    }
    public static QueueAttributeValue visibilityTimeout(Long value) {
        return new QueueAttributeValue("VisibilityTimeout", value.toString());
    }
    public static QueueAttributeValue policy(JsonNode value) {
        return new QueueAttributeValue("Policy", value.toString());
    }
    public static QueueAttributeValue maximumMessageSize(Long value) {
        return new QueueAttributeValue("MaximumMessageSize", value.toString());
    }
    public static QueueAttributeValue messageRetentionPeriod(Long value) {
        return new QueueAttributeValue("MessageRetentionPeriod", value.toString());
    }
    public static QueueAttributeValue delaySeconds(Long value) {
        return new QueueAttributeValue("DelaySeconds", value.toString());
    }

}
