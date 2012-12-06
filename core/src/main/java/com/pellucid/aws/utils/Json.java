package com.pellucid.aws.utils;

import java.io.IOException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class Json {

    public static JsonNode parse(String json) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(json, JsonNode.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
