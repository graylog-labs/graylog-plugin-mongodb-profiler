package com.graylog2.inputs.mongoprofiler.plugin;

import org.graylog2.plugin.PluginMetaData;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.Version;

import java.net.URI;
import java.util.Collections;
import java.util.Set;

public class MongoDBProfilerInputMetadata implements PluginMetaData {
    @Override
    public String getUniqueId() {
        return MongoDBProfilerInputPlugin.class.getCanonicalName();
    }

    @Override
    public String getName() {
        return "MongoDB profiler input";
    }

    @Override
    public String getAuthor() {
        return "Graylog, Inc.";
    }

    @Override
    public URI getURL() {
        return URI.create("http://www.graylog.com/");
    }

    @Override
    public Version getVersion() {
        return new Version(2, 0, 1);
    }

    @Override
    public String getDescription() {
        return "Message input that directly reads profiler information from MongoDB instances";
    }

    @Override
    public Version getRequiredVersion() {
        return new Version(2, 0, 0);
    }

    @Override
    public Set<ServerStatus.Capability> getRequiredCapabilities() {
        return Collections.singleton(ServerStatus.Capability.SERVER);
    }
}
