package com.graylog2.inputs.mongoprofiler.plugin;

import com.graylog2.inputs.mongoprofiler.input.MongoDBProfilerInput;
import com.graylog2.inputs.mongoprofiler.input.mongodb.MongoDBProfilerCodec;
import com.graylog2.inputs.mongoprofiler.input.mongodb.MongoDBProfilerTransport;
import org.graylog2.plugin.PluginModule;

/**
 * @author Lennart Koopmann <lennart@torch.sh>
 */
public class MongoDBProfilerInputModule extends PluginModule {

    @Override
    protected void configure() {
        installCodec(codecMapBinder(), "mongodb-profiler-info", MongoDBProfilerCodec.class);
        installTransport(transportMapBinder(), "mongodb-livetail-collection", MongoDBProfilerTransport.class);
        installInput(inputsMapBinder(), MongoDBProfilerInput.class, MongoDBProfilerInput.Factory.class);
    }

}
