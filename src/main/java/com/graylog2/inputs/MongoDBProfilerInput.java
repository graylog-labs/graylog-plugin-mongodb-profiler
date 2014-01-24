/**
 * Copyright 2014 TORCH GmbH <hello@torch.sh>
 *
 * This file is part of Graylog2 Enterprise.
 *
 */
package com.graylog2.inputs;

import com.google.common.collect.Maps;
import com.mongodb.MongoClient;
import org.graylog2.plugin.configuration.ConfigurationException;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.NumberField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.MisfireException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.Map;

/**
 * @author Lennart Koopmann <lennart@torch.sh>
 */
public class MongoDBProfilerInput extends MessageInput {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBProfilerInput.class);

    private static final String NAME = "MongoDB profiler reader";

    private static final String CK_MONGO_HOST = "mongo_host";
    private static final String CK_MONGO_PORT = "mongo_port";
    private static final String CK_MONGO_DB = "mongo_db";

    private ProfileSubscriber subscriber;

    @Override
    public void launch() throws MisfireException {
        LOG.debug("Launching Mongo DB profiler reader.");

        MongoClient mongoClient;
        try {
            mongoClient = new MongoClient(configuration.getString(CK_MONGO_HOST), (int) configuration.getInt(CK_MONGO_PORT));
        } catch (UnknownHostException e) {
            throw new MisfireException("Could not connect to MongoDB. Unknown host.", e);
        }

        subscriber = new ProfileSubscriber(mongoClient, configuration.getString(CK_MONGO_DB));
        subscriber.start();
    }

    @Override
    public void stop() {
        if (subscriber != null) {
            LOG.debug("Stopping Mongo DB profiler reader");
            subscriber.terminate();
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public ConfigurationRequest getRequestedConfiguration() {
        ConfigurationRequest request = new ConfigurationRequest();

        request.addField(
                new TextField(
                        CK_MONGO_HOST,
                        "MongoDB hostname",
                        "localhost",
                        "The hostname or IP address of the MongoDB instance to connect to.",
                        ConfigurationField.Optional.NOT_OPTIONAL)
        );

        request.addField(
                new NumberField(
                        CK_MONGO_PORT,
                        "MongoDB port",
                        27017,
                        "Port of the MongoDB instance to connect to.",
                        ConfigurationField.Optional.NOT_OPTIONAL,
                        NumberField.Attribute.IS_PORT_NUMBER
                )
        );

        request.addField(
                new TextField(
                        CK_MONGO_DB,
                        "MongoDB database",
                        "",
                        "The name of the profiled MongoDB database.",
                        ConfigurationField.Optional.NOT_OPTIONAL)
        );

        return request;
    }

    @Override
    public void checkConfiguration() throws ConfigurationException {
        if (configuration == null || !configuration.stringIsSet(CK_MONGO_HOST) || !configuration.intIsSet(CK_MONGO_PORT) || !configuration.stringIsSet(CK_MONGO_DB)) {
            throw new ConfigurationException();
        }
    }

    @Override
    public boolean isExclusive() {
        return false;
    }

    @Override
    public String linkToDocs() {
        return "";
    }

    @Override
    public Map<String, Object> getAttributes() {
        return Maps.newHashMap();
    }

}
