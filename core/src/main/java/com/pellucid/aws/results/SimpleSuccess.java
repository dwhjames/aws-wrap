package com.pellucid.aws.results;

public class SimpleSuccess<T> extends SuccessResult<Object, T> implements SimpleResult<T> {

    public SimpleSuccess(T body) {
        super(null, body);
    }

}
