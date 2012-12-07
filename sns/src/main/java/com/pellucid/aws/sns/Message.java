package com.pellucid.aws.sns;

import play.libs.Scala;

public class Message {

    private final String defaultValue;
    private final String http;
    private final String https;
    private final String email;
    private final String emailJson;
    private final String sas;

    public Message(String defaultValue) {
        this(defaultValue, null, null, null, null, null);
    }

    public Message(String defaultValue, String http, String https, String email, String emailJson, String sas) {
        this.defaultValue = defaultValue;
        this.http = http;
        this.https = https;
        this.email = email;
        this.emailJson = emailJson;
        this.sas = sas;
    }

    public Message withHttp(String http) {
        return new Message(
                this.defaultValue,
                http,
                this.https,
                this.email,
                this.emailJson,
                this.sas
                );
    }

    public Message withHttps(String https) {
        return new Message(
                this.defaultValue,
                this.http,
                https,
                this.email,
                this.emailJson,
                this.sas
                );
    }

    public Message withEmail(String email) {
        return new Message(
                this.defaultValue,
                this.http,
                this.https,
                email,
                this.emailJson,
                this.sas
                );
    }

    public Message withEmailJson(String emailJson) {
        return new Message(
                this.defaultValue,
                this.http,
                this.https,
                this.email,
                emailJson,
                this.sas
                );
    }

    public Message withSAS(String sas) {
        return new Message(
                this.defaultValue,
                this.http,
                this.https,
                this.email,
                this.emailJson,
                sas
                );
    }

    public aws.sns.Message toScala() {
        return new aws.sns.Message(
                defaultValue,
                Scala.Option(http),
                Scala.Option(https),
                Scala.Option(email),
                Scala.Option(emailJson),
                Scala.Option(sas)
               );
    }

}
