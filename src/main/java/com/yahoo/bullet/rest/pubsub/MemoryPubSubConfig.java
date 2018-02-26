/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.pubsub;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.Config;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public class MemoryPubSubConfig extends BulletConfig {

    public static final String PREFIX = "bullet.pubsub.memory.";
    // The servlet context path for the in memory pubsub rest endpoints ("api/bullet" by default)
    public static final String CONTEXT_PATH = PREFIX + "context.path";
    // The location (host:port) of this in-memory pubsub instance - used for writeQuery, writeResponse and readResponse
    public static final String SERVER = PREFIX + "server";
    // The locations (comma separated list) of all the in-memory pubsub servers (Bullet backend reads from all pubsub servers)
    public static final String READ_SERVERS = PREFIX + "read.servers";
    // The timeout and retry limits for HTTP connections to in-memory pubsub server
    public static final String CONNECT_TIMEOUT_MS = PREFIX + "connect.timeout.ms";
    public static final String CONNECT_RETRY_LIMIT = PREFIX + "connect.retry.limit";
    // The maxiumum number of allowed uncommitted messages
    public static final String MAX_UNCOMMITTED_MESSAGES = PREFIX + "subscriber.max.uncommitted.messages";

    public static final String DEFAULT_MEMORY_PUBSUB_CONFIGURATION = "pubsub_defaults.yaml";

//    @Autowired
//    private ServletContext servletContext;

    /**
     * Constructor that loads specific file augmented with defaults.
     *
     * @param file YAML file to load.
     */
    public MemoryPubSubConfig(String file) {
        this(new Config(file));
    }

    /**
     * Constructor that loads the defaults and augments it with defaults.
     *
     * @param other The other config to wrap.
     */
    public MemoryPubSubConfig(Config other) {
        // Load Bullet and Storm defaults. Then merge the other.
        super(DEFAULT_MEMORY_PUBSUB_CONFIGURATION);
//        log.error("------ in MemoryPubSubConfig constructor - 1");
//        set(CONTEXT_PATH, "/api/bullet"); // Fix this!
//        log.error("------ in MemoryPubSubConfig constructor - 2");
        merge(other);
        log.info("Merged settings:\n {}", getAll(Optional.empty()));
    }

}
