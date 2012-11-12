package com.pellucid.aws.results;

public interface Result<M, T> {

    public M metadata();

    public boolean isSuccess();

    /**
     * @return the body if success, throws if error if not
     */
    public T body();

}
