/**
 * Copyright 2014 TORCH GmbH <hello@torch.sh>
 *
 * This file is part of Graylog2 Enterprise.
 *
 */
package com.graylog2.inputs.mongoprofiler;

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

    public Normalizer(DBObject obj) {
        this.sortedMap = sort(obj);
    }

    public String getFullHash() {
        StringBuilder sb = new StringBuilder("|");

        // Hell recursion into all the nested levels. #neverForget
        appendFullStringMap(sb, sortedMap);

        sb.append("|");

        return md5(sb.toString());
    }

    public String getFieldsHash() {
        StringBuilder sb = new StringBuilder("|");

        // Hell recursion into all the nested levels. #neverForget
        appendFieldsStringMap(sb, sortedMap);

        sb.append("|");

        return md5(sb.toString());
    }

    private String md5(String x) {
        MessageDigest md;

        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

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
