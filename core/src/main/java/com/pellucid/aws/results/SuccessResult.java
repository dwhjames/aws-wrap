package com.pellucid.aws.results;

public class SuccessResult<M, T> implements Result<M, T> {

    private final T body;
    private final M metadata;

    public SuccessResult(M metadata, T body) {
        this.metadata = metadata;
        this.body = body;
    }

    @Override
    public M metadata() {
        return this.metadata;
    }

    @Override
    public boolean isSuccess() {
        return true;
    }

    @Override
    public T body() {
        return this.body;
    }

    @Override
    public String toString() {
        if (this.body == null) {
            return "SuccessResult: empty";
        }
        return "SuccessResult: " + this.body.toString();
    }

}
