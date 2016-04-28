package com.graylog2.inputs.mongoprofiler.input;

import org.graylog2.plugin.Version;

/**
 * @author Lennart Koopmann <lennart@torch.sh>
 */
public class InputVersion {

    public static final Version PLUGIN_VERSION = new Version(1, 1, 0, "SNAPSHOT");
    public static final Version REQUIRED_VERSION = new Version(2, 0, 0);

}
