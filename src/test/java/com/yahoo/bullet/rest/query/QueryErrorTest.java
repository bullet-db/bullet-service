/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.query;

import org.testng.Assert;
import org.testng.annotations.Test;

public class QueryErrorTest {
    @Test
    public void testCreateNewQueryError() {
        QueryError queryError = new QueryError("foo", "bar");
        Assert.assertEquals(queryError.getError(), "foo");
        Assert.assertEquals(queryError.getResolution(), "bar");
    }
}
