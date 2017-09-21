/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.rest.MockQueryHandler;
import com.yahoo.bullet.rest.resource.QueryError;
import org.testng.Assert;
import org.testng.annotations.Test;

public class QueryHandlerTest {
    @Test
    public void testFailWithServiceUnavailableWhenDefaultFail() {
        MockQueryHandler mockQueryHandler = new MockQueryHandler();
        mockQueryHandler.fail();
        Assert.assertEquals(mockQueryHandler.getError(), QueryError.SERVICE_UNAVAILABLE);
    }
}
