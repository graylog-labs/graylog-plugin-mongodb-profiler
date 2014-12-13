package com.graylog2.inputs.mongoprofiler.input.mongodb.parser;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.bson.types.ObjectId;

import java.io.IOException;

/**
 * @author Lennart Koopmann <lennart@torch.sh>
 */
public class ObjectIdSerializer extends JsonSerializer<ObjectId> {

    @Override
    public void serialize(ObjectId value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
        StringBuilder sb = new StringBuilder();

        sb.append("ObjectId(")
                .append(value.toStringMongod())
                .append(")");

        jgen.writeString(sb.toString());
    }

}
