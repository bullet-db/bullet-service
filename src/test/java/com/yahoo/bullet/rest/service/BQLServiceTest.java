/*
 *  Copyright 2018 Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.bql.BQLResult;
import com.yahoo.bullet.common.BulletError;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

import static com.yahoo.bullet.rest.TestHelpers.assertEqualsQuery;
import static com.yahoo.bullet.rest.TestHelpers.getInvalidBQLQuery;
import static com.yahoo.bullet.rest.TestHelpers.getQueryBuilder;
import static com.yahoo.bullet.rest.TestHelpers.getBQLQuery;

public class BQLServiceTest {
    @Test
    public void testBQLError() {
        BQLService bqlService = new BQLService(getQueryBuilder());
        BQLResult result  = bqlService.toQuery(getInvalidBQLQuery());
        Assert.assertTrue(result.hasErrors());
        List<BulletError> errors = result.getErrors();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertNull(result.getQuery());
    }

    @Test
    public void testBQLConversion() {
        BQLService bqlService = new BQLService(getQueryBuilder());
        BQLResult result  = bqlService.toQuery(getBQLQuery());
        Assert.assertFalse(result.hasErrors());
        Assert.assertNull(result.getErrors());
        assertEqualsQuery(result.getQuery());
    }
}
