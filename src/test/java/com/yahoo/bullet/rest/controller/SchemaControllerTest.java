/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
public class SchemaControllerTest extends AbstractTestNGSpringContextTests {
    @Autowired
    SchemaController controller;

    @Test
    public void testDefaultResponse() {
        String actual = controller.getJSONSchema();

        String expected = "{" +
            "\"data\":" +
                "[" +
                    "{" +
                        "\"id\":\"test\"," +
                        "\"type\":\"column\"," +
                        "\"attributes\":{" +
                            "\"name\":\"test\"," +
                            "\"type\":\"MAP\"," +
                            "\"subtype\":\"STRING\"," +
                            "\"description\":\"foo\"" +
                        "}" +
                    "}" +
                "]," +
            "\"meta\":{\"version\":\"1.2\"}" +
            "}";
        Assert.assertEquals(actual, expected);
    }
}
