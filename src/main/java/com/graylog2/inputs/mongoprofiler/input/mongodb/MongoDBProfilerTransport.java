package com.graylog2.inputs.mongoprofiler.input.mongodb;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import org.graylog2.plugin.LocalMetricRegistry;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.*;
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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Lennart Koopmann <lennart@torch.sh>
 */
public class MongoDBProfilerTransport extends ThrottleableTransport {

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

    private final MetricRegistry localRegistry;

    @AssistedInject
    public MongoDBProfilerTransport(@Assisted final Configuration configuration,
                               final EventBus serverEventBus,
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
        serverStatus.awaitRunning(new Runnable() {
            @Override
            public void run() {
                lifecycleStateChange(Lifecycle.RUNNING);
            }
        });

        serverEventBus.register(this);

        LOG.debug("Launching MongoDB profiler reader.");

        Configuration configuration = input.getConfiguration();
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
                        replicaHosts.add(new ServerAddress(host, configuration.getInt(CK_MONGO_PORT)));
                    }

                    mongoClient = new MongoClient(replicaHosts, credentialList);
                } else {
                    // Authenticated single host.
                    ServerAddress serverAddress = new ServerAddress(
                            mongoHost,
                            configuration.getInt(CK_MONGO_PORT)
                    );

                    mongoClient = new MongoClient(serverAddress, credentialList);
                }
            } else {
                if(mongoHost.contains(",")) {
                    // Unauthenticated replica set.
                    String[] hosts = mongoHost.split(",");
                    List<ServerAddress> replicaHosts = Lists.newArrayList();
                    for(String host : hosts) {
                        replicaHosts.add(new ServerAddress(host, configuration.getInt(CK_MONGO_PORT)));
                    }

                    mongoClient = new MongoClient(replicaHosts);
                } else {
                    // Unauthenticated single host.
                    ServerAddress serverAddress = new ServerAddress(
                            mongoHost,
                            configuration.getInt(CK_MONGO_PORT)
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
                input,
                localRegistry
        );

        subscriber.start();
    }

    @Override
    public void stop() {
        if(subscriber != null) {
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
