/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.query;

import com.yahoo.bullet.common.BulletError;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

public class QueryErrorTest {
    @Test
    public void testCreateNewQueryError() {
        QueryError queryError = new QueryError("foo", "bar");
        List<BulletError> errors = queryError.getErrors();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0).getError(), "foo");
        Assert.assertEquals(errors.get(0).getResolutions(), Collections.singletonList("bar"));
    }
}
