package com.pellucid.aws.sns;

import java.util.List;

import play.libs.Scala;
import scala.collection.JavaConversions;

public class ListTopics {

    private aws.sns.ListTopics sTopics;

    private ListTopics(aws.sns.ListTopics sTopics) {
        this.sTopics = sTopics;
    }

    public static ListTopics fromScala(aws.sns.ListTopics sTopics) {
        return new ListTopics(sTopics);
    }

    public String nextToken() {
        return Scala.orNull(sTopics.nextToken());
    }

    public List<String> topics() {
        return JavaConversions.asJavaList(sTopics.topics());
    }

}
