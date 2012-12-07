package com.pellucid.aws.sns;

public class Endpoint {

    private String protocol;

    private String value;

    public Endpoint(String protocol, String value) {
        this.protocol = protocol;
        this.value = value;
    }

    public String protocol() {
        return this.protocol;
    }

    public String value() {
        return this.value;
    }

    public aws.sns.Endpoint toScala() {
        return aws.sns.Endpoint$.MODULE$.apply(protocol, value);
    }

}
