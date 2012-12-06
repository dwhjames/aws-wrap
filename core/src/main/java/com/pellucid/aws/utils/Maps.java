package com.pellucid.aws.utils;

import java.util.HashMap;
import java.util.Map;

import akka.dispatch.Mapper;

public class Maps {

    public static <A, B, C> Map<A, C> mapValues(Map<A, B> input, Mapper<B, C> convert) {
        Map<A, C> result = new HashMap<A, C>();
        for (A key: input.keySet()) {
            result.put(key, convert.apply(input.get(key)));
        }
        return result;
    }

}
