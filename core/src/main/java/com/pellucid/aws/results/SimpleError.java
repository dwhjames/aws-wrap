package com.pellucid.aws.results;

public class SimpleError extends AWSError<Object> implements SimpleResult<Object> {

    public SimpleError(String code, String message) {
        super(null, code, message);
    }

}
