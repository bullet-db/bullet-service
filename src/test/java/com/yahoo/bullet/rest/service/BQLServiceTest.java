/*
 *  Copyright 2018 Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.bql.BQLResult;
import com.yahoo.bullet.bql.BulletQueryBuilder;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Query;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

public class BQLServiceTest {
    private BQLService bqlService = new BQLService(new BulletQueryBuilder(new BulletConfig()));

    @Test
    public void testBQLError() {
        String bqlQuery = "SELECT * FROM STREAM(3000, TIME) WHERE 1 + 'foo' LIMIT 1;";
        BQLResult result  = bqlService.toQuery(bqlQuery);
        Assert.assertTrue(result.hasErrors());
        List<BulletError> errors = result.getErrors();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertNull(result.getQuery());
    }

    @Test
    public void testBQLConversion() {
        String bqlQuery = "SELECT * FROM STREAM(30000, TIME) LIMIT 1;";
        BQLResult result  = bqlService.toQuery(bqlQuery);
        Assert.assertFalse(result.hasErrors());
        Assert.assertNull(result.getErrors());
        Query query = result.getQuery();
        Assert.assertNull(query.getWindow());
        Assert.assertNull(query.getFilter());
        Assert.assertNull(query.getProjection());
        Assert.assertNull(query.getPostAggregations());
        Assert.assertEquals(query.getDuration(), (Long) 30000L);
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.RAW);
        Assert.assertEquals(query.getAggregation().getSize(), (Integer) 1);
    }
}
