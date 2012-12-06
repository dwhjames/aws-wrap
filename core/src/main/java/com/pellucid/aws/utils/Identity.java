package com.pellucid.aws.utils;

import akka.dispatch.Mapper;

public class Identity<A> extends Mapper<A, A> {

    @Override
    public A apply(A a) {
        return a;
    }

}
