package com.graylog2.inputs.mongoprofiler.input.mongodb.parser;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.graylog2.plugin.journal.RawMessage;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

public class RawParserTest {
    private RawParser rawParser;

    @BeforeMethod
    public void setUp() throws Exception {
        rawParser = new RawParser();
    }

    @Test
    public void testParse() throws Exception {
        final DBObject doc = new BasicDBObject("foo", "bar");
        final RawMessage rawMessage = rawParser.parse(doc);
        assertEquals("mongodb-profiler-info", rawMessage.getCodecName());
        final byte[] payload = rawMessage.getPayload();
        assertTrue(payload.length > 0);
        final ByteArrayInputStream bytes = new ByteArrayInputStream(payload);
        final ObjectInputStream objectInputStream = new ObjectInputStream(bytes);
        final Object o = objectInputStream.readObject();
        assertTrue(o instanceof DBObject);
        final DBObject writtenObject = (DBObject) o;
        assertEquals(doc, writtenObject);
    }
}