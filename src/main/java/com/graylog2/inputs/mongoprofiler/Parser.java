/**
 * Copyright 2014 TORCH GmbH <hello@torch.sh>
 *
 * This file is part of Graylog2 Enterprise.
 *
 */
package com.graylog2.inputs.mongoprofiler;

import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.core.Version;
import com.google.common.collect.Maps;
import com.mongodb.DBObject;
import com.codahale.metrics.MetricRegistry;
import org.bson.types.ObjectId;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.inputs.MessageInput;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * @author Lennart Koopmann <lennart@torch.sh>
 */
public class Parser {

    private static final Logger LOG = LoggerFactory.getLogger(Parser.class);

    private final Timer timer;
    private final ObjectMapper om;

    public Parser(MetricRegistry metrics, MessageInput sourceInput) {
        this.timer = metrics.timer(name(sourceInput.getUniqueReadableId(), "parseTime"));

        this.om = new ObjectMapper();
        SimpleModule bsonModule = new SimpleModule("BSONObjectIdParser", Version.unknownVersion());
        bsonModule.addSerializer(ObjectId.class, new ObjectIdSerializer());
        om.registerModule(bsonModule);
    }

    public Message parse(DBObject doc) throws UnparsableException {
        if (!doc.containsField("op")) {
            LOG.debug("Not parsing profile info with no op.");
            throw new UnparsableException();
        }

        Timer.Context ctx = timer.time();

        Message msg = new Message(
                buildShortMessage(doc),
                "mongoprof",
                new DateTime(doc.get("ts"))
        );

        // Add all fields.
        msg.addFields(getFields(doc));

        // Add our product ID.
        msg.addField("g2eid", MongoDBProfilerInput.G2E_ID);

        ctx.stop();

        return msg;
    }

    private String buildShortMessage(DBObject doc) {
        return new StringBuilder()
                .append(doc.get("op")).append(" ")
                .append(doc.get("ns"))
                .append(" [").append(doc.get("millis")).append("ms]")
                .toString();
    }

    private Map<String, Object> getFields(DBObject doc) {
        String collection = null;
        String database = null;

        /*
         * The "namespace" (ns) is a combination of database.collection.
         * Split it to the interesting parts.
         */
        if (doc.containsField("ns") && ((String) doc.get("ns")).contains(".")) {
            String ns = (String) doc.get("ns");
            int x = ns.indexOf(".");
            database = ns.substring(0, x);
            collection = ns.substring(x+1);
        }

        Map<String, Object> fields = Maps.newHashMap();

        // Standard fields of every op type.
        fields.put("operation", doc.get("op"));
        fields.put("collection", collection);
        fields.put("database", database);
        fields.put("millis", doc.get("millis"));
        fields.put("client", doc.get("client"));
        fields.put("user", doc.get("user"));

        // Query.
        if(doc.containsField("query")) {
            try {
                Normalizer qN = new Normalizer((DBObject) doc.get("query"), database, collection);
                fields.put("query", om.writeValueAsString(doc.get("query")));
                fields.put("query_full_hash", qN.getFullHash());
                fields.put("query_fields_hash", qN.getFieldsHash());
            } catch(JsonProcessingException e) {
                LOG.error("Could not parse MongoDB query to JSON. Not including in fields. Query: " + doc.get("query"), e);
            }
        }

        // Command
        if(doc.containsField("command")) {
            try {
                Normalizer cN = new Normalizer((DBObject) doc.get("command"), database, collection);
                fields.put("command", om.writeValueAsString(doc.get("command")));
                fields.put("query_full_hash", cN.getFullHash());
                fields.put("query_fields_hash", cN.getFieldsHash());
            } catch(JsonProcessingException e) {
                LOG.error("Could not parse MongoDB command to JSON. Not including in fields. Command: " + doc.get("command"), e);
            }
        }

        // Update object.
        if(doc.containsField("updateobj")) {
            try {
                Normalizer uoN = new Normalizer((DBObject) doc.get("updateobj"), database, collection);
                fields.put("update_object", om.writeValueAsString(doc.get("updateobj")));
                fields.put("update_object_full_hash", uoN.getFullHash());
                fields.put("update_object_fields_hash", uoN.getFieldsHash());
            } catch(JsonProcessingException e) {
                LOG.error("Could not parse MongoDB update object to JSON. Not including in fields. Update object: " + doc.get("updateobj"), e);
            }
        }

        // Some of these will/might be NULL.
        fields.put("cursor_id", doc.get("cursorid"));
        fields.put("docs_to_skip", getIntFieldNotZero(doc, "ntoskip"));
        fields.put("docs_to_return", getIntFieldNotZero(doc, "ntoreturn"));
        fields.put("docs_scanned", doc.get("nscanned"));
        fields.put("scan_and_order", doc.get("scanAndOrder"));
        fields.put("moved", doc.get("moved"));
        fields.put("docs_moved", getIntFieldNotZero(doc, "nmoved"));
        fields.put("docs_updated", getIntFieldNotZero(doc, "nupdated"));
        fields.put("index_keys_updated", getIntFieldNotZero(doc, "keyUpdates"));
        fields.put("yields", getIntFieldNotZero(doc, "numYield"));
        fields.put("docs_returned", getIntFieldNotZero(doc, "nreturned"));
        fields.put("response_bytes", doc.get("responseLength"));


        // Lock stats.
        if (doc.containsField("lockStats")) {
            fields.putAll(lockStats(doc));
        }

        return fields;
    }

    private Integer getIntFieldNotZero(DBObject doc, String key) {
        if (!doc.containsField(key)) {
            return null;
        }

        Integer val = (Integer) doc.get(key);

        if(val > 0) {
            return val;
        } else {
            return null;
        }
    }

    private Long getLongFieldNotZero(DBObject doc, String key) {
        if (!doc.containsField(key)) {
            return null;
        }

        Long val = (Long) doc.get(key);

        if(val > 0) {
            return val;
        } else {
            return null;
        }
    }

    public Map<String, Object> lockStats(DBObject doc) {
        Map<String, Object> result = Maps.newHashMap();

        DBObject stats = (DBObject) doc.get("lockStats");
        DBObject timeLocked = (DBObject) stats.get("timeLockedMicros");
        DBObject timeAcquiring = (DBObject) stats.get("timeAcquiringMicros");

        // The time in microseconds the operation held a specific lock.
        result.put("locked_db_read_micros", getLongFieldNotZero(timeLocked, "r"));
        result.put("locked_db_write_micros", getLongFieldNotZero(timeLocked, "w"));
        result.put("locked_global_read_micros", getLongFieldNotZero(timeLocked, "R"));
        result.put("locked_global_write_micros", getLongFieldNotZero(timeLocked, "W"));

        // The time in microseconds the operation spent waiting to acquire a specific lock
        result.put("lockwait_db_read_micros", getLongFieldNotZero(timeAcquiring, "r"));
        result.put("lockwait_db_write_micros", getLongFieldNotZero(timeAcquiring, "w"));
        result.put("lockwait_global_read_micros", getLongFieldNotZero(timeAcquiring, "R"));
        result.put("lockwait_global_write_micros", getLongFieldNotZero(timeAcquiring, "W"));

        return result;
    }

    public class UnparsableException extends Throwable {
    }
}
