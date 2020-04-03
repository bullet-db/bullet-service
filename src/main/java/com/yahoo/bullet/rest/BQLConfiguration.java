/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest;

import com.yahoo.bullet.bql.BQLConfig;
import com.yahoo.bullet.bql.BulletQueryBuilder;
import com.yahoo.bullet.common.BulletConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration @Slf4j
public class BQLConfiguration {
    /**
     * Creates a {@link BQLConfig} for BQL.
     *
     * @param filePath The path to the file to create a {@link com.yahoo.bullet.typesystem.Schema}.
     * @return A created, valid {@link BQLConfig}.
     */
    @Bean
    public BQLConfig bqlConfig(@Value("${bullet.schema.file}") String filePath) {
        BQLConfig config = new BQLConfig();
        log.info("Using {} for the BQL record schema", filePath);
        config.set(BulletConfig.RECORD_SCHEMA_FILE_NAME, filePath);
        config.validate();
        return config;
    }

    /**
     * Creates a query builder for BQL.
     *
     * @param bqlConfig The {@link BulletConfig} to use for instantiating the query builder.
     * @return A created {@link BulletQueryBuilder}.
     */
    @Bean
    public BulletQueryBuilder bulletQueryBuilder(BulletConfig bqlConfig) {
        return new BulletQueryBuilder(bqlConfig);
    }
}
