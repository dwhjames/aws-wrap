package com.pellucid.aws.s3.models;

public class MFA {

    private String serial;

    private String token;

    public MFA(String serial, String token) {
        this.serial = serial;
        this.token = token;
    }

    public String serial() {
        return this.serial;
    }

    public String token() {
        return this.token;
    }

    public aws.s3.S3.MFA toScala() {
        return new aws.s3.S3.MFA(serial, token);
    }

}
