/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.pubsub;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
public class PubSubServiceTest extends AbstractTestNGSpringContextTests {
    @Autowired
    PubSubService service;

    @Test
    public void testPostAndGetQuery() throws Exception {
        String s = service.getQuery();
        Assert.assertNull(s);
        service.postQuery("{\"id\": \"1\", \"sequence\": 1, \"content\": \"foo\", \"metadata\": null}");
        service.postQuery("{\"id\": \"2\", \"sequence\": 2, \"content\": \"bar\", \"metadata\": null}");
        service.postQuery("{\"id\": \"3\", \"sequence\": 3, \"content\": \"baz\", \"metadata\": null}");
        s = service.getQuery();
        Assert.assertEquals(s, "{\"id\": \"1\", \"sequence\": 1, \"content\": \"foo\", \"metadata\": null}");
        s = service.getQuery();
        Assert.assertEquals(s, "{\"id\": \"2\", \"sequence\": 2, \"content\": \"bar\", \"metadata\": null}");
        s = service.getQuery();
        Assert.assertEquals(s, "{\"id\": \"3\", \"sequence\": 3, \"content\": \"baz\", \"metadata\": null}");
        s = service.getQuery();
        Assert.assertNull(s);
    }

    @Test
    public void testPostAndGetResult() throws Exception {
        String s = service.getResult();
        Assert.assertNull(s);
        service.postResult("{\"id\": \"1\", \"sequence\": 1, \"content\": \"foo\", \"metadata\": null}");
        service.postResult("{\"id\": \"2\", \"sequence\": 2, \"content\": \"bar\", \"metadata\": null}");
        service.postResult("{\"id\": \"3\", \"sequence\": 3, \"content\": \"baz\", \"metadata\": null}");
        s = service.getResult();
        Assert.assertEquals(s, "{\"id\": \"1\", \"sequence\": 1, \"content\": \"foo\", \"metadata\": null}");
        s = service.getResult();
        Assert.assertEquals(s, "{\"id\": \"2\", \"sequence\": 2, \"content\": \"bar\", \"metadata\": null}");
        s = service.getResult();
        Assert.assertEquals(s, "{\"id\": \"3\", \"sequence\": 3, \"content\": \"baz\", \"metadata\": null}");
        s = service.getResult();
        Assert.assertNull(s);
    }
}
