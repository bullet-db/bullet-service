/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.metrics.MetricPublisher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricConfiguration {
    /**
     * Creates a {@link MetricPublisher} instance from a provided config.
     *
     * @param config The String path to the config file.
     * @return An instance of the particular {@link MetricPublisher}.
     */
    @Bean
    public MetricPublisher metricPublisher(@Value("${bullet.metric.enabled}") boolean isEnabled,
                                           @Value("${bullet.metric.config}") String config) {
        return isEnabled ? MetricPublisher.from(new BulletConfig(config)) : null;
    }
}
