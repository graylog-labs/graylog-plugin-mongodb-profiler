package com.graylog2.inputs.mongoprofiler.input.mongodb.parser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.graylog2.inputs.mongoprofiler.input.mongodb.normalizer.Normalizer;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.graylog2.plugin.Message;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class Parser {
    private static final Logger LOG = LoggerFactory.getLogger(Parser.class);

    private final ObjectMapper om;

    public Parser() {
        this.om = new ObjectMapper()
                .registerModule(
                        new SimpleModule("BSONObjectIdParser").addSerializer(ObjectId.class, new ObjectIdSerializer())
                );
    }

    public Message parse(DBObject doc) throws UnparsableException {
        if (!doc.containsField("op")) {
            LOG.debug("Not parsing profile info with no op.");
            throw new UnparsableException();
        }

        final Message msg = new Message(buildShortMessage(doc), "mongoprof", new DateTime(doc.get("ts")));

        // Add all fields.
        msg.addFields(getFields(doc));

        return msg;
    }

    private String buildShortMessage(DBObject doc) {
        return String.valueOf(doc.get("op")) + " " + String.valueOf(doc.get("ns")) + " [" + String.valueOf(doc.get("millis")) + "ms]";
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
            collection = ns.substring(x + 1);
        }

        final Map<String, Object> fields = new HashMap<>();

        // Standard fields of every op type.
        fields.put("operation", doc.get("op"));
        fields.put("collection", collection);
        fields.put("database", database);
        fields.put("millis", doc.get("millis"));
        fields.put("client", doc.get("client"));
        fields.put("user", doc.get("user"));

        // Query.
        if (doc.containsField("query")) {
            try {
                final Normalizer normalizer = new Normalizer((DBObject) doc.get("query"), database, collection);
                fields.put("query", om.writeValueAsString(doc.get("query")));
                fields.put("query_full_hash", normalizer.getFullHash());
                fields.put("query_fields_hash", normalizer.getFieldsHash());
            } catch (JsonProcessingException e) {
                LOG.error("Could not parse MongoDB query to JSON. Not including in fields. Query: " + doc.get("query"), e);
            }
        }

        // Command
        if (doc.containsField("command")) {
            try {
                final Normalizer normalizer = new Normalizer((DBObject) doc.get("command"), database, collection);
                fields.put("command", om.writeValueAsString(doc.get("command")));
                fields.put("query_full_hash", normalizer.getFullHash());
                fields.put("query_fields_hash", normalizer.getFieldsHash());
            } catch (JsonProcessingException e) {
                LOG.error("Could not parse MongoDB command to JSON. Not including in fields. Command: " + doc.get("command"), e);
            }
        }

        // Update object.
        if (doc.containsField("updateobj")) {
            try {
                final Normalizer normalizer = new Normalizer((DBObject) doc.get("updateobj"), database, collection);
                fields.put("update_object", om.writeValueAsString(doc.get("updateobj")));
                fields.put("update_object_full_hash", normalizer.getFullHash());
                fields.put("update_object_fields_hash", normalizer.getFieldsHash());
            } catch (JsonProcessingException e) {
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

    @Nullable
    private Integer getIntFieldNotZero(DBObject doc, String key) {
        if (!doc.containsField(key)) {
            return null;
        }

        final Integer val = (Integer) doc.get(key);
        if (val > 0) {
            return val;
        } else {
            return null;
        }
    }

    @Nullable
    private Long getLongFieldNotZero(DBObject doc, String key) {
        if (!doc.containsField(key)) {
            return null;
        }

        final Long val = (Long) doc.get(key);
        if (val > 0) {
            return val;
        } else {
            return null;
        }
    }

    private Map<String, Object> lockStats(DBObject doc) {
        final Map<String, Object> result = new HashMap<>();

        final DBObject stats = (DBObject) doc.get("lockStats");
        final DBObject timeLocked = (DBObject) stats.get("timeLockedMicros");
        final DBObject timeAcquiring = (DBObject) stats.get("timeAcquiringMicros");

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

    public static class UnparsableException extends Exception {
    }
}
