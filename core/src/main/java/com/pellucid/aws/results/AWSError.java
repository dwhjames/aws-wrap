package com.pellucid.aws.results;

public class AWSError<M> implements Result<M, Object> {

    private final String mCode;
    private final String mMessage;
    private final M mMetadata;

    public AWSError(M metadata, String code, String message) {
        this.mMetadata = metadata;
        this.mCode = code;
        this.mMessage = message;
    }

    public String code() {
        return this.mCode;
    }

    public String message() {
        return this.mMessage;
    }

    @Override
    public M metadata() {
        return mMetadata;
    }

    @Override
    public boolean isSuccess() {
        return false;
    }

    @Override
    public Object body() {
        throw new RuntimeException("Error: " + mCode + " - " + mMessage);
    }

    @Override
    public String toString() {
        return "AWSError: " + mCode + " - " + mMessage;
    }

}
