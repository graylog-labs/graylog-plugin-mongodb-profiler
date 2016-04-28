package com.graylog2.inputs.mongoprofiler.input.mongodb;

import com.graylog2.inputs.mongoprofiler.input.mongodb.parser.Parser;
import com.graylog2.inputs.mongoprofiler.input.mongodb.parser.RawParser;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.inputs.codecs.Codec;
import org.graylog2.plugin.journal.RawMessage;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;

public class MongoDBProfilerCodecTest {
    @Mock
    private Parser parser;
    private Codec codec;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        codec = new MongoDBProfilerCodec(parser);
    }

    @Test
    public void testDecode() throws Exception {
        final RawMessage rawMessage = new RawParser().parse(new BasicDBObject("foo", "bar"));
        final Message stubMessage = new Message("foobar", "source", new DateTime(2016, 4, 1, 0, 0, DateTimeZone.UTC));
        when(parser.parse(any(DBObject.class))).thenReturn(stubMessage);
        final Message message = codec.decode(rawMessage);
        assertEquals(stubMessage, message);
    }

    @Test(
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "^Could not de-serialize MongoDB profiler information from raw message.*")
    public void testDecodeInvalidRawMessage() throws Exception {
        final RawMessage rawMessage = new RawMessage(new byte[0]);
        codec.decode(rawMessage);
    }

    @Test(
            expectedExceptions = RuntimeException.class,
            expectedExceptionsMessageRegExp = "^Could not parse MongoDB profiler information.*")
    public void testDecodeInvalidDBObject() throws Exception {
        when(parser.parse(any(DBObject.class))).thenThrow(Parser.UnparsableException.class);
        final RawMessage rawMessage = new RawParser().parse(new BasicDBObject("foo", "bar"));
        codec.decode(rawMessage);
    }

    @Test
    public void testGetName() throws Exception {
        assertEquals("mongodb-profiler-info", codec.getName());
    }

    @Test
    public void testGetConfiguration() throws Exception {
        assertEquals(Configuration.EMPTY_CONFIGURATION, codec.getConfiguration());
    }
}