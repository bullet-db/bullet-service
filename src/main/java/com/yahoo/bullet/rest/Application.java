/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest;

import org.glassfish.jersey.server.ResourceConfig;

public class Application extends ResourceConfig {
    /**
     * Default constructor.
     */
    public Application() {
        packages("com.yahoo.bullet.rest");
    }
}
