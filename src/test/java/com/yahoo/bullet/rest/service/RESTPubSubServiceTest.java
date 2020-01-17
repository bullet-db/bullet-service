/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import org.testng.Assert;
import org.testng.annotations.Test;

public class RESTPubSubServiceTest {
    @Test
    public void testPostAndGetQuery() {
        RESTPubSubService service = new RESTPubSubService();
        String s = service.getQuery();
        Assert.assertNull(s);
        service.postQuery("{'id': '1', 'sequence': 1, 'content': 'foo', 'metadata': null}");
        service.postQuery("{'id': '2', 'sequence': 2, 'content': 'bar', 'metadata': null}");
        service.postQuery("{'id': '3', 'sequence': 3, 'content': 'baz', 'metadata': null}");
        s = service.getQuery();
        Assert.assertEquals(s, "{'id': '1', 'sequence': 1, 'content': 'foo', 'metadata': null}");
        s = service.getQuery();
        Assert.assertEquals(s, "{'id': '2', 'sequence': 2, 'content': 'bar', 'metadata': null}");
        s = service.getQuery();
        Assert.assertEquals(s, "{'id': '3', 'sequence': 3, 'content': 'baz', 'metadata': null}");
        s = service.getQuery();
        Assert.assertNull(s);
    }

    @Test
    public void testPostAndGetResult() {
        RESTPubSubService service = new RESTPubSubService();
        String s = service.getResult();
        Assert.assertNull(s);
        service.postResult("{'id': '1', 'sequence': 1, 'content': 'foo', 'metadata': null}");
        service.postResult("{'id': '2', 'sequence': 2, 'content': 'bar', 'metadata': null}");
        service.postResult("{'id': '3', 'sequence': 3, 'content': 'baz', 'metadata': null}");
        s = service.getResult();
        Assert.assertEquals(s, "{'id': '1', 'sequence': 1, 'content': 'foo', 'metadata': null}");
        s = service.getResult();
        Assert.assertEquals(s, "{'id': '2', 'sequence': 2, 'content': 'bar', 'metadata': null}");
        s = service.getResult();
        Assert.assertEquals(s, "{'id': '3', 'sequence': 3, 'content': 'baz', 'metadata': null}");
        s = service.getResult();
        Assert.assertNull(s);
    }
}
