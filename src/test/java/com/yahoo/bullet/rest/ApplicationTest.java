/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest;

import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
public class ApplicationTest extends AbstractTestNGSpringContextTests {
    @Test
    public void testConfigure() {
        Application application = new Application();
        SpringApplicationBuilder mockBuilder = mock(SpringApplicationBuilder.class);
        application.configure(mockBuilder);
        verify(mockBuilder).sources(eq(Application.class));
    }
}
