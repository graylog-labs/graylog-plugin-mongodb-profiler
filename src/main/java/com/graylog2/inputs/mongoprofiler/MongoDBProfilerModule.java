package com.graylog2.inputs.mongoprofiler;

import org.graylog2.plugin.PluginModule;

public class MongoDBProfilerModule extends PluginModule {
    @Override
    protected void configure() {
        addMessageInput(MongoDBProfilerInput.class);
    }
}
