package com.pellucid.aws.utils;

import java.util.ArrayList;
import java.util.List;

import akka.dispatch.Mapper;

public class Lists {

    public static <A, B> List<B> map(List<A> input, Mapper<A, B> convert) {
        List<B> result = new ArrayList<B>();
        for (A key: input) {
            result.add(convert.apply(key));
        }
        return result;
    }

}
