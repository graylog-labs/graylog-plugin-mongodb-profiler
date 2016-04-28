package com.graylog2.inputs.mongoprofiler.input.mongodb.normalizer;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.mongodb.DBObject;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;

public class Normalizer {
    private final TreeMap<String, Object> sortedMap;
    private final String db;
    private final String collection;

    public Normalizer(DBObject obj, String db, String collection) {
        this.sortedMap = sort(obj);

        this.db = db;
        this.collection = collection;
    }

    public String getFullHash() {
        StringBuilder sb = new StringBuilder("|");

        // Hell recursion into all the nested levels. #neverForget
        appendFullStringMap(sb, sortedMap);

        sb.append("|");

        return hash(sb.toString());
    }

    public String getFieldsHash() {
        StringBuilder sb = new StringBuilder("|");

        // Hell recursion into all the nested levels. #neverForget
        appendFieldsStringMap(sb, sortedMap);

        sb.append("|");

        return hash(sb.toString());
    }

    private String hash(String x) {
        final HashFunction md5 = Hashing.md5();
        /*
         * We are adding the database and collection to the string because
         * we might end up with the same fields hash when querying just for
         * "_id" but in different collections.
         */
        return md5.hashString(x + collection + db, StandardCharsets.UTF_8).toString();
    }

    private TreeMap<String, Object> sort(DBObject obj) {
        final TreeMap<String, Object> sorted = new TreeMap<>();
        for (String key : obj.keySet()) {
            final Object o = obj.get(key);
            if (o instanceof DBObject) {
                sorted.put(key, sort((DBObject) o));
            } else {
                sorted.put(key, o);
            }
        }

        return sorted;
    }

    @SuppressWarnings("unchecked")
    private void appendFullStringMap(StringBuilder sb, TreeMap<String, Object> map) {
        for (Map.Entry<String, Object> x : map.entrySet()) {
            final Object value = x.getValue();
            if (value instanceof TreeMap) {
                sb.append("{");
                appendFullStringMap(sb, (TreeMap<String, Object>) value);
                sb.append("},");
            } else {
                sb.append(x.getKey()).append(":").append(value).append(",");
            }
        }

        // Remove last comma.
        sb.deleteCharAt(sb.toString().length() - 1);
    }

    @SuppressWarnings("unchecked")
    private void appendFieldsStringMap(StringBuilder sb, TreeMap<String, Object> map) {
        for (Map.Entry<String, Object> x : map.entrySet()) {
            final Object value = x.getValue();
            if (value instanceof TreeMap) {
                sb.append("{");
                appendFieldsStringMap(sb, (TreeMap<String, Object>) value);
                sb.append("},");
            } else {
                sb.append(x.getKey()).append(",");
            }
        }

        // Remove last comma.
        sb.deleteCharAt(sb.toString().length() - 1);
    }
}
