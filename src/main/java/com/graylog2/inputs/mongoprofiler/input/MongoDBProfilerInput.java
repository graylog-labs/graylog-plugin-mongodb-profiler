package com.graylog2.inputs.mongoprofiler.input;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.graylog2.inputs.mongoprofiler.input.mongodb.MongoDBProfilerCodec;
import com.graylog2.inputs.mongoprofiler.input.mongodb.MongoDBProfilerTransport;
import org.graylog2.plugin.LocalMetricRegistry;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.annotations.ConfigClass;
import org.graylog2.plugin.inputs.annotations.FactoryClass;

public class MongoDBProfilerInput extends MessageInput {

    private static final String NAME = "MongoDB profiler input";

    @AssistedInject
    public MongoDBProfilerInput(@Assisted Configuration configuration,
                                MetricRegistry metricRegistry,
                                MongoDBProfilerTransport.Factory transport,
                                LocalMetricRegistry localRegistry,
                                MongoDBProfilerCodec.Factory codec,
                                Config config,
                                Descriptor descriptor,
                                ServerStatus serverStatus) {
        super(
                metricRegistry,
                configuration,
                transport.create(configuration),
                localRegistry,
                codec.create(configuration),
                config,
                descriptor,
                serverStatus
        );
    }

    public static class Descriptor extends MessageInput.Descriptor {
        @Inject
        public Descriptor() {
            super(NAME, false, "");
        }
    }

    @ConfigClass
    public static class Config extends MessageInput.Config {
        @Inject
        public Config(MongoDBProfilerTransport.Factory transport, MongoDBProfilerCodec.Factory codec) {
            super(transport.getConfig(), codec.getConfig());
        }
    }

    @FactoryClass
    public interface Factory extends MessageInput.Factory<MongoDBProfilerInput> {
        @Override
        MongoDBProfilerInput create(Configuration configuration);

        @Override
        Config getConfig();

        @Override
        Descriptor getDescriptor();
    }

}
