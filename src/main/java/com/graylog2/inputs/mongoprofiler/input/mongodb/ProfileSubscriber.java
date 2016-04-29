package com.graylog2.inputs.mongoprofiler.input.mongodb;

import com.codahale.metrics.Meter;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Uninterruptibles;
import com.graylog2.inputs.mongoprofiler.input.mongodb.parser.RawParser;
import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.QueryBuilder;
import org.graylog2.plugin.LocalMetricRegistry;
import org.graylog2.plugin.inputs.MessageInput;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.codahale.metrics.MetricRegistry.name;
import static com.google.common.base.Preconditions.checkArgument;

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
        LOG.debug("Connecting ProfileSubscriber.");

        this.stopRequested = new AtomicBoolean(false);

        this.mongoClient = mongoClient;

        this.db = mongoClient.getDB(dbName);

        checkArgument(db.collectionExists("system.profile"), "The \"system.profile\" collection doesn't exist in database \"%s\". Please enable profiling for database \"%s\"", dbName, dbName);
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
            LOG.warn("Profiler collection is not capped. Please enable profiling for database [{}]", this.db.getName());

            final Retryer<Boolean> retryer = RetryerBuilder.<Boolean>newBuilder()
                    .retryIfResult(result -> !result)
                    .retryIfException()
                    .withStopStrategy(StopStrategies.neverStop())
                    .withWaitStrategy(WaitStrategies.exponentialWait(30L, TimeUnit.SECONDS))
                    .build();

            try {
                retryer.call(profile::isCapped);
            } catch (ExecutionException | RetryException e) {
                final Throwable rootCause = Throwables.getRootCause(e);
                Throwables.propagate(rootCause);
            }
        }

        final RawParser rawParser = new RawParser();
        while (!this.stopRequested.get()) {
            LOG.info("Building new cursor.");
            newCursors.mark();
            try (final DBCursor cursor = profile.find(query())
                    .sort(new BasicDBObject("$natural", 1))
                    .addOption(Bytes.QUERYOPTION_TAILABLE)
                    .addOption(Bytes.QUERYOPTION_AWAITDATA)) {
                while (!this.stopRequested.get() && cursor.hasNext()) {
                    cursorReads.mark();

                    if (this.stopRequested.get()) {
                        LOG.info("Stop requested.");
                        return;
                    }

                    try {
                        sourceInput.processRawMessage(rawParser.parse(cursor.next()));
                    } catch (IOException e) {
                        LOG.error("Cannot serialize profile info.", e);
                    } catch (Exception e) {
                        LOG.error("Error when trying to parse profile info.", e);
                    }
                }
            } catch (Exception e) {
                LOG.error("Error when reading MongoDB profile information. Retrying.", e);
            }

            // Something broke if we get here. Retry soonish.
            if (!this.stopRequested.get()) {
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            }
        }
    }

    public void terminate() {
        this.stopRequested.set(true);

        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    private DBObject query() {
        return QueryBuilder
                .start("ts").greaterThan(DateTime.now(DateTimeZone.UTC).toDate())
                .and("ns").notEquals(db.getName() + ".system.profile")
                .get();

    }

}
