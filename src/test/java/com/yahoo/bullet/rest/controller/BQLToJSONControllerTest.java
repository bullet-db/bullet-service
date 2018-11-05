/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.rest.model.BQLToJSONResponse;
import com.yahoo.bullet.rest.query.BQLException;
import com.yahoo.bullet.rest.service.PreprocessingService;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
public class BQLToJSONControllerTest extends AbstractTestNGSpringContextTests {
    @Autowired @InjectMocks
    BQLToJSONController controller;
    @Mock
    PreprocessingService mockPreprocessingService;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testconvertBQLToBulletQuery() throws Exception {
        String bql = "Select * from stream(time, 20000) limit 1;";
        String json = "{\"aggregation\":{\"type\":\"RAW\",\"size\":1},\"duration\":20000}";
        when(mockPreprocessingService.convertIfBQL(bql)).thenReturn(json);
        BQLToJSONResponse response = controller.convertBQLToJSON(bql);

        Assert.assertFalse(response.getHasError());
        Assert.assertEquals(response.getContent(), json);
    }

    @Test
    public void testconvertBQLToBulletQueryWithError() throws Exception {
        String bql = "AAA";
        BQLException bqlException = new BQLException(new RuntimeException("Error"));
        when(mockPreprocessingService.convertIfBQL(bql)).thenThrow(bqlException);
        BQLToJSONResponse response = controller.convertBQLToJSON(bql);

        Assert.assertTrue(response.getHasError());
        Assert.assertEquals(response.getContent(), bqlException.getMessage());
    }
}
