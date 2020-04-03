/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.query;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class QueryErrorTest {
    @Test
    public void testCreateNewQueryError() {
        QueryError queryError = new QueryError("foo", "bar");
        List<BulletError> errors = queryError.getErrors();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0).getError(), "foo");
        Assert.assertEquals(errors.get(0).getResolutions(), singletonList("bar"));
        Clip expected = Clip.of(Meta.of(singletonList(new BulletError("foo", singletonList("bar")))));
        Assert.assertEquals(queryError.toString(), expected.asJSON());
    }

    @Test
    public void testMultipleErrors() {
        List<BulletError> bulletErrors = asList(new BulletError("foo", singletonList("bar")),
                                                new BulletError("baz", singletonList("qux")));
        QueryError queryError = new QueryError(bulletErrors);

        List<BulletError> errors = queryError.getErrors();
        Assert.assertEquals(errors.size(), 2);
        Assert.assertEquals(errors.get(0).getError(), "foo");
        Assert.assertEquals(errors.get(0).getResolutions(), singletonList("bar"));
        Assert.assertEquals(errors.get(1).getError(), "baz");
        Assert.assertEquals(errors.get(1).getResolutions(), singletonList("qux"));

        Assert.assertEquals(queryError.toString(), Clip.of(Meta.of(bulletErrors)).asJSON());
    }
}
