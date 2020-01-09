/*
 *  Copyright 2018 Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.google.gson.JsonSyntaxException;
import com.yahoo.bullet.rest.common.BQLException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.yahoo.bullet.TestHelpers.assertJSONEquals;
import static org.mockito.Mockito.doReturn;

public class PreprocessingServiceTest {
    @Test
    public void testConvertIfBQLDoesNothingToJSON() throws Exception {
        PreprocessingService preprocessingService = new PreprocessingService(new HandlerService(), 500);
        String query = "{}";
        String newQuery = preprocessingService.convertIfBQL(query);
        assertJSONEquals(newQuery, query);
    }

    @Test
    public void testConvertIfBQLConverts() throws Exception {
        PreprocessingService preprocessingService = new PreprocessingService(new HandlerService(), 500);
        String query = "SELECT * FROM STREAM(30000, TIME) LIMIT 1;";
        String newQuery = preprocessingService.convertIfBQL(query);
        assertJSONEquals(newQuery, "{'aggregation':{'size':1,'type':'RAW'},'duration':30000}");
    }

    @Test(expectedExceptions = BQLException.class)
    public void testConvertIfBQLThrowsIfQueryBad() throws Exception {
        PreprocessingService preprocessingService = new PreprocessingService(new HandlerService(), 500);
        String query = "garbage";
        preprocessingService.convertIfBQL(query);
    }

    @Test
    public void testContainsWindowHasWindow() {
        PreprocessingService preprocessingService = new PreprocessingService(new HandlerService(), 500);
        String query = "{'window':{'emit':{'type': 'TIME','every': 5000}}}";
        Assert.assertTrue(preprocessingService.containsWindow(query));
    }

    @Test
    public void testContainsWindowHasNoWindow() {
        PreprocessingService preprocessingService = new PreprocessingService(new HandlerService(), 500);
        String query = "{'filters':[{'field':'demog_info','operation':'!=','values':['null']}]}";
        Assert.assertFalse(preprocessingService.containsWindow(query));
    }

    @Test(expectedExceptions = JsonSyntaxException.class)
    public void testContainsWindowThrows() {
        PreprocessingService preprocessingService = new PreprocessingService(new HandlerService(), 500);
        String query = "garbage";
        preprocessingService.containsWindow(query);
    }

    @Test
    public void testQueryLimitReached() {
        HandlerService handlerService = Mockito.mock(HandlerService.class);
        doReturn(500).when(handlerService).count();
        PreprocessingService preprocessingService = new PreprocessingService(handlerService, 500);
        Assert.assertTrue(preprocessingService.queryLimitReached());
    }

    @Test
    public void testQueryLimitNotReached() {
        HandlerService handlerService = Mockito.mock(HandlerService.class);
        doReturn(499).when(handlerService).count();
        PreprocessingService preprocessingService = new PreprocessingService(handlerService, 500);
        Assert.assertFalse(preprocessingService.queryLimitReached());
    }
}
