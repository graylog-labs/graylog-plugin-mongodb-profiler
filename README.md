MongoDB Profiler for Graylog
============================

[![Build Status](https://travis-ci.org/Graylog2/graylog-plugin-mongodb-profiler.svg)](https://travis-ci.org/Graylog2/graylog-plugin-mongodb-profiler)

If you are unfamiliar with Graylog or MongoDB you should read this more
detailed blog post instead:

https://www.graylog.org/troubleshoot-slow-mongodb-queries-in-minutes-with-graylog/

**Required Graylog version:** 2.0.0 and later

Please use version 1.0.1 of this plugin if you are still running Graylog 1.x

![](https://github.com/Graylog2/graylog-plugin-mongodb-profiler/blob/master/1.png)

![](https://github.com/Graylog2/graylog-plugin-mongodb-profiler/blob/master/2.png)

## Installation & Usage

1. [Download the plugin](https://github.com/Graylog2/graylog-plugin-mongodb-profiler/releases)
and place the `.jar` file in your Graylog plugin directory. The plugin directory
is the `plugins/` folder relative from your `graylog-server` directory by default
and can be configured in your `graylog.conf` file.
1. Restart `graylog-server`
1. Start a new MongoDB profiler input from System -> Inputs in your Graylog Web Interface
1. Make sure to [enable the profiler in your MongoDB processes](https://docs.mongodb.org/v3.0/tutorial/manage-the-database-profiler/). Set it to at least level 1 or 2.


## Build

This project is using Maven 3 and requires Java 8 or higher.

You can build a plugin (JAR) with `mvn package`.

DEB and RPM packages can be build with `mvn jdeb:jdeb` and `mvn rpm:rpm` respectively.

## Plugin Release

We are using the maven release plugin:

```
$ mvn release:prepare
[...]
$ mvn release:perform
```

This sets the version numbers, creates a tag and pushes to GitHub. Travis CI will build the release artifacts and upload to GitHub automatically.
