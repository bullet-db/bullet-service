/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.storage.StorageManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class StorageConfiguration {
    /**
     * Creates a StorageManager instance from a provided config.
     *
     * @param config The String path to the config file.
     * @return An instance of the particular {@link StorageManager} indicated in the config.
     */
    @Bean
    public StorageManager storageManager(@Value("${bullet.storage.config}") String config) {
        return StorageManager.from(new BulletConfig(config));
    }
}
