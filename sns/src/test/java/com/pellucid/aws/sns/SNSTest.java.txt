package com.pellucid.aws.sns;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.*;

import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import com.pellucid.aws.results.Result;

public class SNSTest {

    private final static Duration timeout = Duration.create("30 seconds");

    private final static SNS sns = new SNS(SNSRegion.EU_WEST_1);

    private <T> void checkResult(Result<SNSMeta, T> result) {
        assertTrue(result.toString(), result.isSuccess());
    }

    @Test
    public void createDeleteTopic() throws Exception {
        Result<SNSMeta, String> result = Await.result(sns.createTopic("java-create-topic"), timeout);
        checkResult(result);
        String topicArn = result.body();
        checkResult(Await.result(sns.deleteTopic(topicArn), timeout));
    }

    @Test
    public void listTopics() throws Exception {
        Result<SNSMeta, String> result = Await.result(sns.createTopic("java-list-topic"), timeout);
        checkResult(result);
        String topicArn = result.body();

        Result<SNSMeta, ListTopics> listTopicsResults = Await.result(sns.listTopics(), timeout);
        checkResult(listTopicsResults);
        boolean found = false;
        for (String topic: listTopicsResults.body().topics()) {
            if (topic.equals(topicArn)) found = true;
        }
        assertTrue("Couldn't find the newly created topic in the list", found);

        checkResult(Await.result(sns.deleteTopic(topicArn), timeout));
    }

    @Test
    public void subscribe() throws Exception {
        Result<SNSMeta, String> result = Await.result(sns.createTopic("java-subscribe"), timeout);
        checkResult(result);
        String topicArn = result.body();

        Result<SNSMeta, String> subscribeRes = Await.result(sns.createTopic("java-subscribe"), timeout);
        checkResult(subscribeRes);

        checkResult(Await.result(sns.deleteTopic(topicArn), timeout));
    }

    @Test
    public void publish() throws Exception {
        Result<SNSMeta, String> result = Await.result(sns.createTopic("java-subscribe"), timeout);
        checkResult(result);
        String topicArn = result.body();

        Message message = (new Message("hello, there")).withHttp("just for http");
        Result<SNSMeta, String> publishRes = Await.result(sns.publish(topicArn, message), timeout);
        checkResult(publishRes);

        checkResult(Await.result(sns.deleteTopic(topicArn), timeout));
    }

    // Add / remove permissions
    @Test
    public void addRemovePermissions() throws Exception {
        Result<SNSMeta, String> result = Await.result(sns.createTopic("java-permissions"), timeout);
        assertTrue(result.toString(), result.isSuccess());
        String topicArn = result.body();

        List<String> accounts = new ArrayList<String>();
        accounts.add("foobar@example.com");
        List<Action> actions = new ArrayList<Action>();
        actions.add(Action.ListTopics);
        checkResult(Await.result(sns.addPermission(topicArn, "Foobar", accounts, actions), timeout));
        checkResult(Await.result(sns.removePermission(topicArn, "Foobar"), timeout));

        checkResult(Await.result(sns.deleteTopic(topicArn), timeout));
    }

    // Set attributes

}
