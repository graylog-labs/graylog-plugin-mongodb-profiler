/**
 * Copyright 2014 TORCH GmbH <hello@torch.sh>
 *
 * This file is part of Graylog2 Enterprise.
 *
 */
package com.graylog2.inputs.mongoprofiler;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Lennart Koopmann <lennart@torch.sh>
 */
public class NormalizerTest {

    @Test
    public void testSimpleHash() throws Exception {
        DBObject dbo = new BasicDBObject("stream_id", "12345678abc");
        Normalizer n = new Normalizer(dbo);

        assertEquals("189cf4b41c6ea04e19a255c3d360466b", n.getFullHash());
        assertEquals("c6296b9acab9696be7a45871a815468b", n.getFieldsHash());
    }

    @Test
    public void testSimpleHashWithNumber() throws Exception {
        DBObject dbo = new BasicDBObject("stream_id", 9001);
        Normalizer n = new Normalizer(dbo);

        assertEquals("b1754def5db4adee35a25c639da5bd93", n.getFullHash());
        assertEquals("c6296b9acab9696be7a45871a815468b", n.getFieldsHash());

    }

    @Test
    public void testSimpleHashWithBoolean() throws Exception {
        DBObject dbo = new BasicDBObject("has_stream", true);
        Normalizer n = new Normalizer(dbo);

        assertEquals("fb567d7d66c9c5afaff2922c7b6342be", n.getFullHash());
        assertEquals("fbea9f2632b5244529be39cd4f731c63", n.getFieldsHash());
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

        Normalizer n1 = new Normalizer(dbo1);
        Normalizer n2 = new Normalizer(dbo2);

        assertEquals("88aee2995b1c91bab6dbfa1cf7ad43e6", n1.getFullHash());
        assertEquals("88aee2995b1c91bab6dbfa1cf7ad43e6", n2.getFullHash());

        assertEquals("91ed0270c36ad217aab02852972499e8", n1.getFieldsHash());
        assertEquals("91ed0270c36ad217aab02852972499e8", n2.getFieldsHash());
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

        Normalizer n1 = new Normalizer(dbo1);
        Normalizer n2 = new Normalizer(dbo2);

        assertEquals("6e4f0ac5dd1f2f497f55f27a8e253963", n1.getFullHash());
        assertEquals("6e4f0ac5dd1f2f497f55f27a8e253963", n2.getFullHash());

        assertEquals("5dff59c59c292b1781baa7c7929e83a1", n1.getFieldsHash());
        assertEquals("5dff59c59c292b1781baa7c7929e83a1", n2.getFieldsHash());
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

        Normalizer n1 = new Normalizer(dbo1);
        Normalizer n2 = new Normalizer(dbo2);

        assertEquals("91ed0270c36ad217aab02852972499e8", n1.getFieldsHash());
        assertEquals("91ed0270c36ad217aab02852972499e8", n2.getFieldsHash());
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

        Normalizer n1 = new Normalizer(dbo1);
        Normalizer n2 = new Normalizer(dbo2);

        assertEquals("5dff59c59c292b1781baa7c7929e83a1", n1.getFieldsHash());
        assertEquals("5dff59c59c292b1781baa7c7929e83a1", n2.getFieldsHash());
    }

}
