package com.graylog2.inputs.mongoprofiler.input.mongodb.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.bson.types.ObjectId;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

public class ObjectIdSerializerTest {
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new SimpleModule("test").addSerializer(ObjectId.class, new ObjectIdSerializer()));

    @Test
    public void testSerialize() throws Exception {
        final ObjectId objectId = new ObjectId("deadbeefcafebabe12345678");
        assertEquals("\"ObjectId(deadbeefcafebabe12345678)\"", objectMapper.writeValueAsString(objectId));
    }
}