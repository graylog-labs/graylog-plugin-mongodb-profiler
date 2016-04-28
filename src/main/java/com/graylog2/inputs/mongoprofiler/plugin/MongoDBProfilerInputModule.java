package com.graylog2.inputs.mongoprofiler.plugin;

import com.graylog2.inputs.mongoprofiler.input.MongoDBProfilerInput;
import com.graylog2.inputs.mongoprofiler.input.mongodb.MongoDBProfilerCodec;
import com.graylog2.inputs.mongoprofiler.input.mongodb.MongoDBProfilerTransport;
import org.graylog2.plugin.PluginModule;

public class MongoDBProfilerInputModule extends PluginModule {
    @Override
    protected void configure() {
        addCodec("mongodb-profiler-info", MongoDBProfilerCodec.class);
        addTransport("mongodb-livetail-collection", MongoDBProfilerTransport.class);
        addMessageInput(MongoDBProfilerInput.class);
    }
}
