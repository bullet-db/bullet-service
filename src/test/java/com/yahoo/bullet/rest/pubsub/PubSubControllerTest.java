/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.pubsub;

import com.yahoo.bullet.rest.Mocks.MockHttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;
import javax.servlet.http.HttpServletResponse;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
public class PubSubControllerTest extends AbstractTestNGSpringContextTests {
    @Autowired
    PubSubController controller;

    @Test
    public void testPostAndGetQuery() throws Exception {
        HttpServletResponse response = new MockHttpServletResponse();
        controller.postQuery("{\"id\": \"88\", \"sequence\": -1, \"content\": \"foo\", \"metadata\": null}");
        String result = controller.getQuery(response);
        Assert.assertEquals(response.getStatus(), 200);
        Assert.assertEquals(result, "{\"id\":\"88\",\"sequence\":-1,\"content\":\"foo\",\"metadata\":null}");
        controller.getQuery(response);
        Assert.assertEquals(response.getStatus(), 204);
    }

    @Test
    public void testPostAndGetResult() throws Exception {
        HttpServletResponse response = new MockHttpServletResponse();
        controller.postResult("{\"id\": \"88\", \"sequence\": -1, \"content\": \"foo\", \"metadata\": null}");
        String result = controller.getResult(response);
        Assert.assertEquals(response.getStatus(), 200);
        Assert.assertEquals(result, "{\"id\":\"88\",\"sequence\":-1,\"content\":\"foo\",\"metadata\":null}");
        controller.getQuery(response);
        Assert.assertEquals(response.getStatus(), 204);
    }
}
