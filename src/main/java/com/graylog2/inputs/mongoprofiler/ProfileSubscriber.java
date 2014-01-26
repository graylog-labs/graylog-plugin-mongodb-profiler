/**
 * Copyright 2014 TORCH GmbH <hello@torch.sh>
 *
 * This file is part of Graylog2 Enterprise.
 *
 */
package com.graylog2.inputs.mongoprofiler;

import com.mongodb.*;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.buffers.Buffer;
import org.graylog2.plugin.inputs.MessageInput;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Lennart Koopmann <lennart@torch.sh>
 */
public class ProfileSubscriber extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(ProfileSubscriber.class);

    private final MongoClient mongoClient;
    private final DB db;
    private final DBCollection profile;

    private final Parser parser;

    private final MessageInput sourceInput;
    private final Buffer targetBuffer;

    private boolean stopRequested = false;

    public ProfileSubscriber(MongoClient mongoClient, String dbName, Buffer targetBuffer, MessageInput sourceInput) {
        LOG.info("Connecting ProfileSubscriber.");

        this.mongoClient = mongoClient;

        this.db = mongoClient.getDB(dbName);
        this.profile = db.getCollection("system.profile");

        if(!this.profile.isCapped()) {
            throw new RuntimeException("The system.profile collections seems to be not capped.");
        }

        parser = new Parser();

        this.targetBuffer = targetBuffer;
        this.sourceInput = sourceInput;
    }

    @Override
    public void run() {
        while(!stopRequested) {
            try {
                LOG.info("Building new cursor.");
                DBCursor cursor = profile.find(query())
                        .sort(new BasicDBObject("$natural", 1))
                        .addOption(Bytes.QUERYOPTION_TAILABLE)
                        .addOption(Bytes.QUERYOPTION_AWAITDATA);

                try {
                    while(cursor.hasNext()) {
                        if (stopRequested) {
                            LOG.info("Stop requested.");
                            return;
                        }

                        try {
                            Message message = parser.parse(cursor.next());

                            targetBuffer.insertCached(message, sourceInput);
                        } catch(Parser.UnparsableException e) {
                            LOG.error("Cannot parse profile info.", e);
                            continue;
                        } catch(Exception e) {
                            LOG.error("Error when trying to parse profile info.", e);
                            continue;
                        }
                    }
                } finally {
                    if (cursor != null) {
                        cursor.close();
                    }
                }
            } catch (Exception e) {
                LOG.error("Error when reading MongoDB profile information. Retrying.", e);
            }

            // Something broke if we get here. Retry soonish.
            try {
                if(!stopRequested) { Thread.sleep(2500); }
            } catch (InterruptedException e) { break; }
        }
    }

    public void terminate() {
        stopRequested = true;

        if(mongoClient != null) {
            mongoClient.close();
        }
    }

    public DBObject query() {
        return QueryBuilder
                .start("ts").greaterThan(DateTime.now(DateTimeZone.UTC).toDate())
                .and("ns").notEquals(db.getName() + ".system.profile")
                .get();

    }

}
