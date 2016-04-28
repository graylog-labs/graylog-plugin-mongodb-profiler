package com.graylog2.inputs.mongoprofiler.input.mongodb.parser;

import com.mongodb.DBObject;
import org.graylog2.plugin.journal.RawMessage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class RawParser {
    public RawMessage parse(DBObject doc) throws IOException {
        try (final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             final ObjectOutputStream out = new ObjectOutputStream(byteOut)) {
            out.writeObject(doc);

            final RawMessage message = new RawMessage(byteOut.toByteArray());
            message.setCodecName("mongodb-profiler-info");

            return message;
        }
    }
}
