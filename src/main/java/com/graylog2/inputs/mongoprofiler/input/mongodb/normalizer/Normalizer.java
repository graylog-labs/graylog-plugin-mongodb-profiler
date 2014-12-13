package com.graylog2.inputs.mongoprofiler.input.mongodb.normalizer;

import com.google.common.collect.Maps;
import com.mongodb.DBObject;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author Lennart Koopmann <lennart@torch.sh>
 */
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
        MessageDigest md;

        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        /*
         * We are adding the database and collection to the string because
         * we might end up with the same fields hash when querying just for
         * "_id" but in different collections.
         */
        x += collection+db;

        byte[] digestresult = md.digest(x.getBytes());
        StringBuffer hex = new StringBuffer();

        for (int i = 0; i < digestresult.length; i++) {
            if ((0xff & digestresult[i]) < 0x10) {
                hex.append("0").append(Integer.toHexString((0xFF & digestresult[i])));
            } else {
                hex.append(Integer.toHexString(0xFF & digestresult[i]));
            }
        }

        return hex.toString();
    }

    private TreeMap sort(DBObject obj) {
        TreeMap sorted = Maps.newTreeMap();

        for(String key : obj.keySet()) {
            Object o = obj.get(key);

            if (o instanceof DBObject) {
                sorted.put(key, sort((DBObject) o));
            } else {
                sorted.put(key, o);
            }
        }

        return sorted;
    }

    public void appendFullStringMap(StringBuilder sb, TreeMap<String, Object> map) {
        for (Map.Entry<String, Object> x : map.entrySet()) {
            if (x.getValue() instanceof TreeMap) {
                sb.append("{");
                appendFullStringMap(sb, (TreeMap) x.getValue());
                sb.append("},");
            } else {
                sb.append(x.getKey()).append(":").append(x.getValue()).append(",");
            }
        }

        // Remove last comma.
        sb.deleteCharAt(sb.toString().length()-1);
    }

    public void appendFieldsStringMap(StringBuilder sb, TreeMap<String, Object> map) {
        for (Map.Entry<String, Object> x : map.entrySet()) {
            if (x.getValue() instanceof TreeMap) {
                sb.append("{");
                appendFieldsStringMap(sb, (TreeMap) x.getValue());
                sb.append("},");
            } else {
                sb.append(x.getKey()).append(",");
            }
        }

        // Remove last comma.
        sb.deleteCharAt(sb.toString().length()-1);
    }

}
