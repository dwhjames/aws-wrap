package com.pellucid.aws.sns;

import org.junit.Test;

import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import com.pellucid.aws.results.Result;

import org.junit.*;
import static org.junit.Assert.*;

public class SNSTest {

    private final static Duration timeout = Duration.create("30 seconds");

    private final static SNS sns = new SNS(SNSRegion.EU_WEST_1);

    @Test
    public void createDeleteTopic() throws Exception {
        Result<SNSMeta, String> result = Await.result(sns.createTopic("java-create-topic"), timeout);
        assertTrue(result.toString(), result.isSuccess());
        String topicArn = result.body();
        Result<SNSMeta, Object> result2 = Await.result(sns.deleteTopic(topicArn), timeout);
        assertTrue(result2.toString(), result2.isSuccess());
    }

    @Test
    public void listTopics() throws Exception {
        Result<SNSMeta, String> result = Await.result(sns.createTopic("java-list-topic"), timeout);
        assertTrue(result.toString(), result.isSuccess());
        String topicArn = result.body();

        Result<SNSMeta, ListTopics> listTopicsResults = Await.result(sns.listTopics(), timeout);
        assertTrue(listTopicsResults.toString(), listTopicsResults.isSuccess());
        boolean found = false;
        for (String topic: listTopicsResults.body().topics()) {
            if (topic.equals(topicArn)) found = true;
        }
        assertTrue("Couldn't find the newly created topic in the list", found);

        Result<SNSMeta, Object> result2 = Await.result(sns.deleteTopic(topicArn), timeout);
        assertTrue(result2.toString(), result2.isSuccess());
    }

    @Test
    public void subscribe() throws Exception {
        Result<SNSMeta, String> result = Await.result(sns.createTopic("java-subscribe"), timeout);
        assertTrue(result.toString(), result.isSuccess());
        String topicArn = result.body();

        Result<SNSMeta, String> subscribeRes = Await.result(sns.createTopic("java-subscribe"), timeout);
        assertTrue(subscribeRes.toString(), subscribeRes.isSuccess());

        Result<SNSMeta, Object> result2 = Await.result(sns.deleteTopic(topicArn), timeout);
        assertTrue(result2.toString(), result2.isSuccess());
    }

    // Publish

    // Add / remove permissions

    // Set attributes

}
