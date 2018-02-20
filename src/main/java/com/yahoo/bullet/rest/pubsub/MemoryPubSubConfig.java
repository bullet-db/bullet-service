/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.pubsub;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.Config;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import javax.servlet.ServletContext;

import java.util.Optional;

@Slf4j
public class MemoryPubSubConfig extends BulletConfig {

    public static final String PREFIX = "bullet.pubsub.memory.";
    // The servlet context path for the in memory pubsub rest endpoints ("api/bullet" by default)
    public static final String SERVLET_CONTEXT = PREFIX + "servlet.context";
    // The location of the in-memory pubsub server
    public static final String SERVER = PREFIX + "server";

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
        log.error("------ in MemoryPubSubConfig constructor - 1");
        set(SERVLET_CONTEXT, "/api/bullet");
        log.error("------ in MemoryPubSubConfig constructor - 2");
        merge(other);
        log.info("Merged settings:\n {}", getAll(Optional.empty()));
    }

}
