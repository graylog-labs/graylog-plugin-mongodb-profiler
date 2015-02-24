package com.graylog2.inputs.mongoprofiler.input;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.graylog2.inputs.mongoprofiler.input.mongodb.MongoDBProfilerCodec;
import com.graylog2.inputs.mongoprofiler.input.mongodb.MongoDBProfilerTransport;
import com.graylog2.inputs.mongoprofiler.plugin.MongoDBProfilerInputMetadata;
import com.graylog2.inputs.mongoprofiler.plugin.MongoDBProfilerInputModule;
import org.graylog2.plugin.*;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.codecs.Codec;
import org.graylog2.plugin.inputs.transports.Transport;

import java.util.Collection;

/**
 * @author Lennart Koopmann <lennart@torch.sh>
 */
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

    public static class Config extends MessageInput.Config {
        @Inject
        public Config(MongoDBProfilerTransport.Factory transport, MongoDBProfilerCodec.Factory codec) {
            super(transport.getConfig(), codec.getConfig());
        }
    }

    public interface Factory extends MessageInput.Factory<MongoDBProfilerInput> {
        @Override
        MongoDBProfilerInput create(Configuration configuration);

        @Override
        Config getConfig();

        @Override
        Descriptor getDescriptor();
    }

}
