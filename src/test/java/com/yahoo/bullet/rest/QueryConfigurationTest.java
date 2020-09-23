/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest;

import com.yahoo.bullet.bql.BQLConfig;
import com.yahoo.bullet.bql.BulletQueryBuilder;
import com.yahoo.bullet.common.BulletConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

public class QueryConfigurationTest {
    @Test
    public void testConfigurationDefault() {
        QueryConfiguration configuration = new QueryConfiguration();
        BQLConfig config = configuration.bqlConfig(null);
        Assert.assertNotNull(config);
        Assert.assertEquals(config.getAs(BulletConfig.RECORD_SCHEMA_FILE_NAME, String.class), null);
        Assert.assertEquals(config.getAs(BulletConfig.AGGREGATION_MAX_SIZE, Integer.class),
                            (Integer) BulletConfig.DEFAULT_AGGREGATION_MAX_SIZE);
    }

    @Test
    public void testConfiguration() {
        QueryConfiguration configuration = new QueryConfiguration();
        BQLConfig config = configuration.bqlConfig("test_query_defaults.yaml");
        Assert.assertNotNull(config);
        Assert.assertEquals(config.getAs(BulletConfig.RECORD_SCHEMA_FILE_NAME, String.class), "sample_fields.json");
        Assert.assertEquals(config.getAs(BulletConfig.AGGREGATION_MAX_SIZE, Integer.class), (Integer) 1000);
    }

    @Test
    public void testQueryBuilder() {
        QueryConfiguration configuration = new QueryConfiguration();
        BulletQueryBuilder queryBuilder = configuration.bulletQueryBuilder(configuration.bqlConfig(null));
        Assert.assertNotNull(queryBuilder);
    }
}
