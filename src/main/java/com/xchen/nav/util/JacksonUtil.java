package com.xchen.nav.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JacksonUtil<T> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private JacksonUtil() {}

    public static <T> T parseJson(String json, Class<T> clazz) throws JsonProcessingException {
            return objectMapper.readValue(json, clazz);
    }
}
