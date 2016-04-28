package com.graylog2.inputs.mongoprofiler.input.mongodb;

import com.codahale.metrics.Meter;
import com.graylog2.inputs.mongoprofiler.input.mongodb.parser.RawParser;
import com.mongodb.*;
import org.graylog2.plugin.LocalMetricRegistry;
import org.graylog2.plugin.inputs.MessageInput;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * @author Lennart Koopmann <lennart@torch.sh>
 */
public class ProfileSubscriber extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(ProfileSubscriber.class);

    private final MongoClient mongoClient;
    private final DB db;
    private final DBCollection profile;

    private final MessageInput sourceInput;

    private final Meter newCursors;
    private final Meter cursorReads;

    private AtomicBoolean stopRequested;

    public ProfileSubscriber(MongoClient mongoClient, String dbName, MessageInput sourceInput, LocalMetricRegistry metricRegistry) {
        LOG.info("Connecting ProfileSubscriber.");

        this.stopRequested = new AtomicBoolean(false);

        this.mongoClient = mongoClient;

        this.db = mongoClient.getDB(dbName);
        this.profile = db.getCollection("system.profile");

        this.sourceInput = sourceInput;

        String metricName = sourceInput.getUniqueReadableId();
        this.cursorReads = metricRegistry.meter(name(metricName, "cursorReads"));
        this.newCursors = metricRegistry.meter(name(metricName, "newCursors"));
    }

    @Override
    public void run() {
        // Wait until the collection is ready. (It is capped after profiling is turned on)
        if (!this.profile.isCapped()) {
            LOG.debug("Profiler collection is not capped. Please enable profiling for database [{}]", this.db.getName());
            while (true) {
                if (this.profile.isCapped()) {
                    LOG.info("Profiler collection is capped. Moving on.");
                    break;
                } else {
                    LOG.debug("Profiler collection is not capped.");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        }

        RawParser rawParser = new RawParser();

        while (!this.stopRequested.get()) {
            try {
                LOG.info("Building new cursor.");
                newCursors.mark();

                DBCursor cursor = profile.find(query())
                        .sort(new BasicDBObject("$natural", 1))
                        .addOption(Bytes.QUERYOPTION_TAILABLE)
                        .addOption(Bytes.QUERYOPTION_AWAITDATA);

                try {
                    while (!this.stopRequested.get() && cursor.hasNext()) {
                        cursorReads.mark();

                        if (this.stopRequested.get()) {
                            LOG.info("Stop requested.");
                            return;
                        }

                        try {
                            sourceInput.processRawMessage(rawParser.parse(cursor.next(), sourceInput));
                        } catch (IOException e) {
                            LOG.error("Cannot serialize profile info.", e);
                            continue;
                        } catch (Exception e) {
                            LOG.error("Error when trying to parse profile info.", e);
                            continue;
                        }
                    }
                } finally {
                    if (cursor != null && !this.stopRequested.get()) {
                        cursor.close();
                    }
                }
            } catch (Exception e) {
                LOG.error("Error when reading MongoDB profile information. Retrying.", e);
            }

            // Something broke if we get here. Retry soonish.
            try {
                if (!this.stopRequested.get()) {
                    Thread.sleep(2500);
                }
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    public void terminate() {
        this.stopRequested.set(true);

        if (mongoClient != null) {
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
