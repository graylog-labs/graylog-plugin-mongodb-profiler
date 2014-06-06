package com.graylog2.inputs.mongoprofiler;

import com.beust.jcommander.internal.Lists;
import org.graylog2.plugin.Plugin;
import org.graylog2.plugin.PluginModule;

import java.util.Collection;
import java.util.List;

public class MongoDBProfilerPlugin implements Plugin {

    public Collection<PluginModule> modules() {
        List<PluginModule> result = Lists.newArrayList();
        result.add(new MongoDBProfilerModule());
        return result;
    }
}
