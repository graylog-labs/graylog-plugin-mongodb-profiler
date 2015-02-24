package com.graylog2.inputs.mongoprofiler.plugin;

import com.graylog2.inputs.mongoprofiler.input.InputVersion;
import org.graylog2.plugin.PluginMetaData;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.Version;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Lennart Koopmann <lennart@torch.sh>
 */
public class MongoDBProfilerInputMetadata implements PluginMetaData {

    @Override
    public String getUniqueId() {
        /*
         * I have no idea what I'm doing. What is a
         * "unique ID" in this context? using a UUID
         * for now.
         */
        return "c8237800-825b-11e4-b4a9-0800200c9a66";
    }

    @Override
    public String getName() {
        return "MongoDB profiler input";
    }

    @Override
    public String getAuthor() {
        return "Lennart Koopmann";
    }

    @Override
    public URI getURL() {
        try {
            return new URI("http://www.graylog.com/");
        } catch (URISyntaxException ignored) {
            throw new RuntimeException("Invalid plugin source URI.", ignored);
        }
    }

    @Override
    public Version getVersion() {
        return InputVersion.PLUGIN_VERSION;
    }

    @Override
    public String getDescription() {
        return "Message input that directly reads profiler information from MongoDB instances";
    }

    @Override
    public Version getRequiredVersion() {
        return InputVersion.REQUIRED_VERSION;
    }

    @Override
    public Set<ServerStatus.Capability> getRequiredCapabilities() {
        return new HashSet<ServerStatus.Capability>(){{
            add(ServerStatus.Capability.SERVER);
        }};
    }

}
