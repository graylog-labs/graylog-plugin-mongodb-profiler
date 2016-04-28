package com.graylog2.inputs.mongoprofiler.input.mongodb.normalizer;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

/**
 * @author Lennart Koopmann <lennart@torch.sh>
 */
public class NormalizerTest {

    @Test
    public void testSimpleHash() throws Exception {
        DBObject dbo = new BasicDBObject("stream_id", "12345678abc");
        Normalizer n = new Normalizer(dbo, "db", "coll");

        assertEquals("23ae16c6c521a9481450439715a5222a", n.getFullHash());
        assertEquals("f29c81f1601cbf3fd843283da6a67cd5", n.getFieldsHash());
    }

    @Test
    public void testSimpleHashWithNumber() throws Exception {
        DBObject dbo = new BasicDBObject("stream_id", 9001);
        Normalizer n = new Normalizer(dbo, "db", "coll");

        assertEquals("24d3a63ba4a56fbb5c359fd3375cf4f8", n.getFullHash());
        assertEquals("f29c81f1601cbf3fd843283da6a67cd5", n.getFieldsHash());
    }

    @Test
    public void testSimpleHashWithBoolean() throws Exception {
        DBObject dbo = new BasicDBObject("has_stream", true);
        Normalizer n = new Normalizer(dbo, "db", "coll");

        assertEquals("efc823e6e92ac6c84d4c1d68d9377b8f", n.getFullHash());
        assertEquals("27a4aceb03a1471d8388ef418014b3c0", n.getFieldsHash());
    }

    @Test
    public void testOrderingOfHash() throws Exception {
        DBObject dbo1 = new BasicDBObject();
        dbo1.put("stream_id", "12345678abc");
        dbo1.put("username", "lennart");
        dbo1.put("x", "y");
        dbo1.put("foo", "bar");

        DBObject dbo2 = new BasicDBObject();
        dbo2.put("username", "lennart");
        dbo2.put("foo", "bar");
        dbo2.put("stream_id", "12345678abc");
        dbo2.put("x", "y");

        Normalizer n1 = new Normalizer(dbo1, "db", "coll");
        Normalizer n2 = new Normalizer(dbo2, "db", "coll");

        assertEquals("ef4fefee3f427f200fecdcc6bbe9a3d1", n1.getFullHash());
        assertEquals("ef4fefee3f427f200fecdcc6bbe9a3d1", n2.getFullHash());

        assertEquals("c8fef00c8830633aa912c3490353d8ae", n1.getFieldsHash());
        assertEquals("c8fef00c8830633aa912c3490353d8ae", n2.getFieldsHash());
    }

    @Test
    public void testMultiLevelOrderingOfHash() throws Exception {
        DBObject dbo1 = new BasicDBObject();
        dbo1.put("stream_id", "12345678abc");
        dbo1.put("username", "lennart");
        dbo1.put("x", "y");
        dbo1.put("foo", "bar");

        DBObject deep1_1 = new BasicDBObject();
        DBObject deep1_2 = new BasicDBObject();
        deep1_2.put("$gt", 5);
        deep1_2.put("$lt", 11);
        deep1_1.put("wat", "wut");
        deep1_1.put("why", "because");
        deep1_1.put("num", deep1_2);

        dbo1.put("sodeep", deep1_1);

        DBObject dbo2 = new BasicDBObject();
        dbo2.put("username", "lennart");
        dbo2.put("foo", "bar");
        dbo2.put("stream_id", "12345678abc");
        dbo2.put("x", "y");

        DBObject deep2_1 = new BasicDBObject();
        DBObject deep2_2 = new BasicDBObject();
        deep2_2.put("$lt", 11);
        deep2_2.put("$gt", 5);
        deep2_1.put("wat", "wut");
        deep2_1.put("num", deep1_2);
        deep2_1.put("why", "because");

        dbo2.put("sodeep", deep2_1);

        Normalizer n1 = new Normalizer(dbo1, "db", "coll");
        Normalizer n2 = new Normalizer(dbo2, "db", "coll");

        assertEquals("474fa0d9b8d4ba9f82044e30c24ccd12", n1.getFullHash());
        assertEquals("474fa0d9b8d4ba9f82044e30c24ccd12", n2.getFullHash());

        assertEquals("a983077409137ef798a8d27fa560efd3", n1.getFieldsHash());
        assertEquals("a983077409137ef798a8d27fa560efd3", n2.getFieldsHash());
    }

    @Test
    public void testOrderingAndValueIndifferenceOfFieldsHash() throws Exception {
        DBObject dbo1 = new BasicDBObject();
        dbo1.put("stream_id", "12345678abc");
        dbo1.put("username", "lennart");
        dbo1.put("x", "y");
        dbo1.put("foo", "bar");

        DBObject dbo2 = new BasicDBObject();
        dbo2.put("username", "kay");
        dbo2.put("foo", "bar");
        dbo2.put("stream_id", 9001);
        dbo2.put("x", "y");

        Normalizer n1 = new Normalizer(dbo1, "db", "coll");
        Normalizer n2 = new Normalizer(dbo2, "db", "coll");

        assertEquals("c8fef00c8830633aa912c3490353d8ae", n1.getFieldsHash());
        assertEquals("c8fef00c8830633aa912c3490353d8ae", n2.getFieldsHash());
    }

    @Test
    public void testMultiLevelOrderingAndValueIndifferenceOfFieldsHash() throws Exception {
        DBObject dbo1 = new BasicDBObject();
        dbo1.put("stream_id", "12345678abc");
        dbo1.put("username", "lennart");
        dbo1.put("x", "y");
        dbo1.put("foo", "bar");

        DBObject deep1_1 = new BasicDBObject();
        DBObject deep1_2 = new BasicDBObject();
        deep1_2.put("$gt", 5);
        deep1_2.put("$lt", 11);
        deep1_1.put("wat", "fdsfsd");
        deep1_1.put("why", "gfdgfd");
        deep1_1.put("num", deep1_2);

        dbo1.put("sodeep", deep1_1);

        DBObject dbo2 = new BasicDBObject();
        dbo2.put("username", "lennart");
        dbo2.put("foo", "bar");
        dbo2.put("stream_id", "12345678abc");
        dbo2.put("x", "y");

        DBObject deep2_1 = new BasicDBObject();
        DBObject deep2_2 = new BasicDBObject();
        deep2_2.put("$lt", 9001);
        deep2_2.put("$gt", 2);
        deep2_1.put("wat", "wut");
        deep2_1.put("num", deep1_2);
        deep2_1.put("why", "ewfsdf");

        dbo2.put("sodeep", deep2_1);

        Normalizer n1 = new Normalizer(dbo1, "db", "coll");
        Normalizer n2 = new Normalizer(dbo2, "db", "coll");

        assertEquals("a983077409137ef798a8d27fa560efd3", n1.getFieldsHash());
        assertEquals("a983077409137ef798a8d27fa560efd3", n2.getFieldsHash());
    }

    @Test
    public void testSameQueryBuildsDifferentFieldsHashOnDifferentDb() throws Exception {
        DBObject dbo1 = new BasicDBObject();
        dbo1.put("stream_id", "12345678abc");
        dbo1.put("username", "lennart");
        dbo1.put("x", "y");

        DBObject dbo2 = new BasicDBObject();
        dbo2.put("stream_id", "12345678abc");
        dbo2.put("username", "lennart");
        dbo2.put("x", "y");

        Normalizer n1 = new Normalizer(dbo1, "db", "coll");
        Normalizer n2 = new Normalizer(dbo2, "db2", "coll");

        assertFalse(n1.getFieldsHash().equals(n2.getFieldsHash()));
    }

    @Test
    public void testSameQueryBuildsDifferentFieldsHashOnDifferentCollection() throws Exception {
        DBObject dbo1 = new BasicDBObject();
        dbo1.put("stream_id", "12345678abc");
        dbo1.put("username", "lennart");
        dbo1.put("x", "y");

        DBObject dbo2 = new BasicDBObject();
        dbo2.put("stream_id", "12345678abc");
        dbo2.put("username", "lennart");
        dbo2.put("x", "y");

        Normalizer n1 = new Normalizer(dbo1, "db", "coll");
        Normalizer n2 = new Normalizer(dbo2, "db", "coll2");

        assertFalse(n1.getFieldsHash().equals(n2.getFieldsHash()));
    }

    @Test
    public void testSameQueryBuildsDifferentHashOnDifferentDb() throws Exception {
        DBObject dbo1 = new BasicDBObject();
        dbo1.put("stream_id", "12345678abc");
        dbo1.put("username", "lennart");
        dbo1.put("x", "y");

        DBObject dbo2 = new BasicDBObject();
        dbo2.put("stream_id", "12345678abc");
        dbo2.put("username", "lennart");
        dbo2.put("x", "y");

        Normalizer n1 = new Normalizer(dbo1, "db", "coll");
        Normalizer n2 = new Normalizer(dbo2, "db2", "coll");

        assertFalse(n1.getFullHash().equals(n2.getFullHash()));
    }

    @Test
    public void testSameQueryBuildsDifferentHashOnDifferentCollection() throws Exception {
        DBObject dbo1 = new BasicDBObject();
        dbo1.put("stream_id", "12345678abc");
        dbo1.put("username", "lennart");
        dbo1.put("x", "y");

        DBObject dbo2 = new BasicDBObject();
        dbo2.put("stream_id", "12345678abc");
        dbo2.put("username", "lennart");
        dbo2.put("x", "y");

        Normalizer n1 = new Normalizer(dbo1, "db", "coll");
        Normalizer n2 = new Normalizer(dbo2, "db", "coll2");

        assertFalse(n1.getFullHash().equals(n2.getFullHash()));
    }

}
