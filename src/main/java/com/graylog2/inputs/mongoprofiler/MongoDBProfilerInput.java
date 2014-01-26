/**
 * Copyright 2014 TORCH GmbH <hello@torch.sh>
 *
 * This file is part of Graylog2 Enterprise.
 *
 */
package com.graylog2.inputs.mongoprofiler;

import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import org.graylog2.plugin.configuration.ConfigurationException;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.BooleanField;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.NumberField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.MisfireException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
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
    private static final String CK_MONGO_USE_AUTH = "mongo_use_auth";
    private static final String CK_MONGO_USER = "mongo_user";
    private static final String CK_MONGO_PW = "mongo_password";

    private ProfileSubscriber subscriber;

    @Override
    public void launch() throws MisfireException {
        LOG.debug("Launching Mongo DB profiler reader.");

        String mongoHost = configuration.getString(CK_MONGO_HOST);

        MongoClient mongoClient;
        try {
            if (configuration.getBoolean(CK_MONGO_USE_AUTH)) {
                final MongoCredential credentials = MongoCredential.createMongoCRCredential(
                        configuration.getString(CK_MONGO_USER),
                        configuration.getString(CK_MONGO_DB),
                        configuration.getString(CK_MONGO_PW).toCharArray()
                );

                List<MongoCredential> credentialList = new ArrayList<MongoCredential>(){{ add(credentials); }};

                if(mongoHost.contains(",")) {
                    // Authenticated replica set.
                    String[] hosts = mongoHost.split(",");
                    List<ServerAddress> replicaHosts = Lists.newArrayList();
                    for(String host : hosts) {
                        replicaHosts.add(new ServerAddress(host, (int) configuration.getInt(CK_MONGO_PORT)));
                    }

                    mongoClient = new MongoClient(replicaHosts, credentialList);
                } else {
                    // Authenticated single host.
                    ServerAddress serverAddress = new ServerAddress(
                            mongoHost,
                            (int) configuration.getInt(CK_MONGO_PORT)
                    );

                    mongoClient = new MongoClient(serverAddress, credentialList);
                }
            } else {
                if(mongoHost.contains(",")) {
                    // Unauthenticated replica set.
                    String[] hosts = mongoHost.split(",");
                    List<ServerAddress> replicaHosts = Lists.newArrayList();
                    for(String host : hosts) {
                        replicaHosts.add(new ServerAddress(host, (int) configuration.getInt(CK_MONGO_PORT)));
                    }

                    mongoClient = new MongoClient(replicaHosts);
                } else {
                    // Unauthenticated single host.
                    ServerAddress serverAddress = new ServerAddress(
                            mongoHost,
                            (int) configuration.getInt(CK_MONGO_PORT)
                    );

                    mongoClient = new MongoClient(serverAddress);
                }
            }
        } catch (UnknownHostException e) {
            throw new MisfireException("Could not connect to MongoDB. Unknown host.", e);
        }

        // Try the connection.
        try {
            mongoClient.getDB(configuration.getString(CK_MONGO_DB)).getStats();
        } catch (Exception e) {
            throw new MisfireException("Could not verify MongoDB profiler connection.", e);
        }

        subscriber = new ProfileSubscriber(
                mongoClient,
                configuration.getString(CK_MONGO_DB),
                graylogServer.getProcessBuffer(),
                this,
                graylogServer
        );

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
                        "The hostname or IP address of the MongoDB instance to connect to. You can also supply comma separated hosts when using a replica set.",
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

        request.addField(
                new BooleanField(
                        CK_MONGO_USE_AUTH,
                        "Use authentication?",
                        false,
                        "Use MongoDB authentication?"
                )
        );

        request.addField(
                new TextField(
                        CK_MONGO_USER,
                        "MongoDB user",
                        "",
                        "MongoDB username. Only used if authentication is enabled.",
                        ConfigurationField.Optional.OPTIONAL)
        );

        request.addField(
                new TextField(
                        CK_MONGO_PW,
                        "MongoDB password",
                        "",
                        "MongoDB password. Only used if authentication is enabled. Note that this is stored unencrypted",
                        ConfigurationField.Optional.OPTIONAL,
                        TextField.Attribute.IS_PASSWORD
                )
        );

        return request;
    }

    @Override
    public void checkConfiguration() throws ConfigurationException {
        if (configuration == null
                || !configuration.stringIsSet(CK_MONGO_HOST)
                || !configuration.intIsSet(CK_MONGO_PORT)
                || !configuration.stringIsSet(CK_MONGO_DB)) {
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
        return configuration.getSource();
    }

}
