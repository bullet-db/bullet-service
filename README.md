# bullet-service
The Web-Service layer for Bullet

[![Build Status](https://travis-ci.org/yahoo/bullet-service.svg?branch=master)](https://travis-ci.org/yahoo/bullet-service) [![Coverage Status](https://coveralls.io/repos/github/yahoo/bullet-service/badge.svg?branch=master)](https://coveralls.io/github/yahoo/bullet-service?branch=master) [![Download](https://api.bintray.com/packages/yahoo/maven/bullet-service/images/download.svg) ](https://bintray.com/yahoo/maven/bullet-service/_latestVersion)

This project builds a war file that you can deploy on a machine to communicate with the Bullet Storm Topology through DRPC. See
the [bullet-storm](https://github.com/yahoo/bullet-storm) repo.

There are two main purposes for this layer at this time:

1) It provides an endpoint that can serve a [JSON API schema](http://jsonapi.org/format/) for the Bullet UI. Currently, static schemas from a file are supported.

2) It proxies a JSON Bullet query to the Bullet Storm topology and wraps errors if the topology is unreachable.

The web-service oto be a point of abstraction for implementing things like security, monitoring, access-control,
rate-limiting, different query formats (e.g. SQL Bullet queries) etc.

## Installation

You need to deploy the war file to your servlet container. Jetty is recommended. You can get the war file directly from [JCenter](http://jcenter.bintray.com/com/yahoo/bullet/bullet-service/).

If you need to depend on the source code directly, you need to add the JCenter repository and get the artifact through your dependency management system. Maven is shown below.

```xml
    <repositories>
        <repository>
            <snapshots>
                <enabled>false</enabled>
            </snapshots>
            <id>central</id>
            <name>bintray</name>
            <url>http://jcenter.bintray.com</url>
        </repository>
    </repositories>
```

```xml
    <dependency>
      <groupId>com.yahoo.bullet</groupId>
      <artifactId>bullet-service</artifactId>
      <version>${bullet.version}</version>
    </dependency>
```

## Usage

You deploy the war file into your favorite servlet container (has been tested on Jetty 9).

You can HTTP POST a Bullet query to:
```
http://<HOST>:<PORT>/<contextPath>/api/drpc
```

, where contextPath is generally the name of the war in Jetty.

If you provided a schema file, you can also HTTP GET your schema at:

```
http://<HOST>:<PORT>/<contextPath>/api/columns
```

This is the endpoint you would provide to your Bullet UI for it to know about your data schema. Note that you do not necessarily have to use this web-service to serve your schema. The UI
can use any JSON API schema specification. You can use [src/main/resources/sample_columns.json](src/main/resources/sample_columns.json) as a guideline for what it should look like.

## Configuration

You specify how to talk to your Bullet Storm instance and where to find your schema file through a configuration file. See sample at
[src/main/resources/bullet_defaults.yaml](src/main/resources/bullet_defaults.yaml).

The values in the defaults file are used for any missing properties. You can specify a path to your custom configuration using the property:
```
bullet.service.configuration.file=<path to your configuration file>
```

For example, if you are using Jetty as your servlet container,

```
java -jar -Dbullet.service.configuration.file=/var/bullet-service/context.properties start.jar
```

See [src/main/resources/ApplicationContext.xml](src/main/resources/ApplicationContext.xml) for how this is loaded.

Code licensed under the Apache 2 license. See LICENSE file for terms.
