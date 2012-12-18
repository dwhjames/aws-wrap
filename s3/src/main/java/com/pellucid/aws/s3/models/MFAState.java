package com.pellucid.aws.s3.models;

import scala.Tuple2;

public class MFAState {

    public static enum DeleteState {
        DISABLED, ENABLED
    }

    private String serial;
    private String token;
    private DeleteState state;

    public MFAState(String serial, String token, DeleteState state) {
        this.serial = serial;
        this.token = token;
        this.state = state;
    }

    public String serial() {
        return serial;
    }

    public String token() {
        return token;
    }

    public DeleteState state() {
        return state;
    }

    public Tuple2<scala.Enumeration.Value, aws.s3.S3.MFA> toScala() {
        scala.Enumeration.Value scalaState = (state == DeleteState.DISABLED)
                ? aws.s3.S3.MFADeleteStates$.MODULE$.DISABLED()
                        : aws.s3.S3.MFADeleteStates$.MODULE$.ENABLED();
        return new Tuple2(scalaState, new aws.s3.S3.MFA(serial, token));
    }
}
