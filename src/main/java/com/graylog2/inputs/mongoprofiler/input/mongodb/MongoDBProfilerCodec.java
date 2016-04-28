package com.graylog2.inputs.mongoprofiler.input.mongodb;

import com.google.common.annotations.VisibleForTesting;
import com.graylog2.inputs.mongoprofiler.input.mongodb.parser.Parser;
import com.mongodb.DBObject;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.inputs.annotations.ConfigClass;
import org.graylog2.plugin.inputs.annotations.FactoryClass;
import org.graylog2.plugin.inputs.codecs.Codec;
import org.graylog2.plugin.inputs.codecs.CodecAggregator;
import org.graylog2.plugin.journal.RawMessage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

import static java.util.Objects.requireNonNull;

public class MongoDBProfilerCodec implements Codec {
    private final Parser parser;

    public MongoDBProfilerCodec() {
        this(new Parser());
    }

    @VisibleForTesting
    protected MongoDBProfilerCodec(final Parser parser) {
        this.parser = requireNonNull(parser);
    }

    @Nullable
    @Override
    public Message decode(@Nonnull RawMessage rawMessage) {
        final DBObject doc;
        try (
                final ByteArrayInputStream b = new ByteArrayInputStream(rawMessage.getPayload());
                final ObjectInputStream o = new ObjectInputStream(b)) {
            doc = (DBObject) o.readObject();
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not de-serialize MongoDB profiler information from raw message. Skipping.", e);
        }

        try {
            return parser.parse(doc);
        } catch (Parser.UnparsableException e) {
            throw new RuntimeException("Could not parse MongoDB profiler information. Skipping.", e);
        }
    }

    @Nullable
    @Override
    public CodecAggregator getAggregator() {
        return null;
    }

    @Override
    public String getName() {
        return "mongodb-profiler-info";
    }

    @Nonnull
    @Override
    public Configuration getConfiguration() {
        return Configuration.EMPTY_CONFIGURATION;
    }

    @FactoryClass
    public interface Factory extends Codec.Factory<MongoDBProfilerCodec> {
        @Override
        MongoDBProfilerCodec create(Configuration configuration);

        @Override
        Config getConfig();
    }

    @ConfigClass
    public static class Config implements Codec.Config {
        @Override
        public ConfigurationRequest getRequestedConfiguration() {
            return new ConfigurationRequest();
        }

        @Override
        public void overrideDefaultValues(@Nonnull ConfigurationRequest cr) {
        }
    }

}
