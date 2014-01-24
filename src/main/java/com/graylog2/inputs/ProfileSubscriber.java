/**
 * Copyright 2014 Lennart Koopmann <lennart@torch.sh>
 *
 * This file is part of Graylog2.
 *
 * Graylog2 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog2 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog2.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.graylog2.inputs;

import com.mongodb.*;
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

    private boolean stopRequested = false;

    public ProfileSubscriber(MongoClient mongoClient, String dbName) {
        LOG.info("Connection ProfileSubscriber.");

        this.mongoClient = mongoClient;

        this.db = mongoClient.getDB(dbName);
        this.profile = db.getCollection("system.profile");

        if(!this.profile.isCapped()) {
            throw new RuntimeException("The system.profile collections seems to be not capped.");
        }
    }

    @Override
    public void run() {
        while(!stopRequested) {
            try {
                LOG.info("Building new cursor.");
                DBCursor cursor = profile.find()
                        .sort(new BasicDBObject("$natural", 1))
                        .addOption(Bytes.QUERYOPTION_TAILABLE)
                        .addOption(Bytes.QUERYOPTION_AWAITDATA);

                try {
                    while(cursor.hasNext()) {
                        if (stopRequested) {
                            LOG.info("Stop requested.");
                            break;
                        }

                        DBObject doc = cursor.next();

                        // We do not want to log our own tail queries.
                        if (doc.containsField("ns") && ((String) doc.get("ns")).endsWith(".system.profile")){
                            continue;
                        }

                        System.out.println(doc);
                    }
                } finally {
                    if (cursor != null) {
                        cursor.close();
                    }
                }
            } catch (Exception e) {
                LOG.error("Error when reading MongoDB profile information. Retrying.");
            }

            // Something broke if we get here. Retry soonish.
            try {
                if(!stopRequested) {
                    LOG.info("Stop requested.");
                    Thread.sleep(2500);
                }
            } catch (InterruptedException e) { break; }
        }
    }

    public void terminate() {
        stopRequested = true;

        if(mongoClient != null) {
            mongoClient.close();
        }
    }

}
