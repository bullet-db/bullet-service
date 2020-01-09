/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest;

import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.testng.annotations.Test;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class APIConfigurationTest {
    @Test
    public void testCORSConfiguration() {
        APIConfiguration configuration = new APIConfiguration();
        WebMvcConfigurer corsConfigurer = configuration.corsConfigurer();
        CorsRegistry mockRegistry = mock(CorsRegistry.class);
        corsConfigurer.addCorsMappings(mockRegistry);
        verify(mockRegistry).addMapping(eq("/**"));
    }
}
