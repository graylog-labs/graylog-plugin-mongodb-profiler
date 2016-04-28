package com.graylog2.inputs.mongoprofiler.input.mongodb;

import com.codahale.metrics.MetricSet;
import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.inject.assistedinject.AssistedInject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import org.graylog2.plugin.LocalMetricRegistry;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.BooleanField;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.NumberField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.MisfireException;
import org.graylog2.plugin.inputs.annotations.ConfigClass;
import org.graylog2.plugin.inputs.annotations.FactoryClass;
import org.graylog2.plugin.inputs.codecs.CodecAggregator;
import org.graylog2.plugin.inputs.transports.ThrottleableTransport;
import org.graylog2.plugin.inputs.transports.Transport;
import org.graylog2.plugin.lifecycles.Lifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MongoDBProfilerTransport implements Transport {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBProfilerTransport.class);

    private static final String CK_MONGO_HOST = "mongo_host";
    private static final String CK_MONGO_PORT = "mongo_port";
    private static final String CK_MONGO_DB = "mongo_db";
    private static final String CK_MONGO_USE_AUTH = "mongo_use_auth";
    private static final String CK_MONGO_USER = "mongo_user";
    private static final String CK_MONGO_PW = "mongo_password";

    private final EventBus serverEventBus;
    private final ServerStatus serverStatus;

    private ProfileSubscriber subscriber;

    private final LocalMetricRegistry localRegistry;

    @AssistedInject
    public MongoDBProfilerTransport(final EventBus serverEventBus,
                                    final LocalMetricRegistry localRegistry,
                                    final ServerStatus serverStatus) {
        this.localRegistry = localRegistry;
        this.serverEventBus = serverEventBus;
        this.serverStatus = serverStatus;
    }

    @Override
    public void setMessageAggregator(CodecAggregator codecAggregator) {
        // Not supported.
    }

    @Subscribe
    public void lifecycleStateChange(Lifecycle lifecycle) {
        LOG.debug("Lifecycle changed to {}", lifecycle);
        switch (lifecycle) {
            case PAUSED:
            case FAILED:
            case HALTING:
                if (subscriber != null) {
                    subscriber.terminate();
                }
                break;
            default:
                if (subscriber != null) {
                    subscriber.terminate();
                }
                break;
        }
    }

    @Override
    public void launch(MessageInput input) throws MisfireException {
        serverStatus.awaitRunning(() -> lifecycleStateChange(Lifecycle.RUNNING));
        serverEventBus.register(this);

        LOG.debug("Launching MongoDB profiler reader.");
        final Configuration configuration = input.getConfiguration();

        final int port = configuration.getInt(CK_MONGO_PORT);
        final MongoClient mongoClient;
        final List<MongoCredential> credentialList;
        final String db = configuration.getString(CK_MONGO_DB);
        if (configuration.getBoolean(CK_MONGO_USE_AUTH)) {
            final MongoCredential credentials = MongoCredential.createCredential(
                    configuration.getString(CK_MONGO_USER),
                    db,
                    configuration.getString(CK_MONGO_PW).toCharArray()
            );

            credentialList = ImmutableList.of(credentials);
        } else {
            credentialList = ImmutableList.of();
        }

        final String mongoHost = configuration.getString(CK_MONGO_HOST);
        final String[] hosts = mongoHost.split(",");
        final List<ServerAddress> replicaHosts = new ArrayList<>(hosts.length);
        for (String host : hosts) {
            replicaHosts.add(new ServerAddress(host, port));
        }
        mongoClient = new MongoClient(replicaHosts, credentialList);

        // Try the connection.
        try {
            mongoClient.getDB(db).getStats();
        } catch (Exception e) {
            throw new MisfireException("Could not verify MongoDB profiler connection.", e);
        }

        subscriber = new ProfileSubscriber(mongoClient, db, input, localRegistry);
        subscriber.start();
    }

    @Override
    public void stop() {
        if (subscriber != null) {
            subscriber.terminate();
        }

        serverEventBus.unregister(this);
    }

    @FactoryClass
    public interface Factory extends Transport.Factory<MongoDBProfilerTransport> {
        @Override
        MongoDBProfilerTransport create(Configuration configuration);

        @Override
        Config getConfig();
    }

    @ConfigClass
    public static class Config extends ThrottleableTransport.Config {
        @Override
        public ConfigurationRequest getRequestedConfiguration() {
            final ConfigurationRequest request = super.getRequestedConfiguration();

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
    }

    @Override
    public MetricSet getMetricSet() {
        return localRegistry;
    }
}
