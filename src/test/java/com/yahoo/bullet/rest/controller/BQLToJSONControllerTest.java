/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.rest.common.BQLException;
import com.yahoo.bullet.rest.model.BQLToJSONResponse;
import com.yahoo.bullet.rest.service.PreprocessingService;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BQLToJSONControllerTest {
    @Test
    public void testConvertBQLToBulletQuery() throws Exception {
        PreprocessingService service = mock(PreprocessingService.class);
        BQLToJSONController controller = new BQLToJSONController(service);
        String bql = "Select * from stream(time, 20000) limit 1;";
        String json = "{\"aggregation\":{\"type\":\"RAW\",\"size\":1},\"duration\":20000}";
        when(service.convertIfBQL(bql)).thenReturn(json);
        BQLToJSONResponse response = controller.convertBQLToJSON(bql);

        Assert.assertFalse(response.getHasError());
        Assert.assertEquals(response.getContent(), json);
    }

    @Test
    public void testConvertBQLToBulletQueryWithError() throws Exception {
        PreprocessingService service = mock(PreprocessingService.class);
        BQLToJSONController controller = new BQLToJSONController(service);
        String bql = "AAA";
        BQLException bqlException = new BQLException(new RuntimeException("Error"));
        when(service.convertIfBQL(bql)).thenThrow(bqlException);
        BQLToJSONResponse response = controller.convertBQLToJSON(bql);

        Assert.assertTrue(response.getHasError());
        Assert.assertEquals(response.getContent(), bqlException.getMessage());
    }
}
