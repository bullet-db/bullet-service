/*
 *  Copyright 2018 Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.google.gson.JsonSyntaxException;
import com.yahoo.bullet.rest.query.BQLException;
import com.yahoo.bullet.rest.query.QueryHandler;
import com.yahoo.bullet.rest.query.TooManyQueriesException;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;
import org.testng.Assert;
import java.util.concurrent.ConcurrentMap;
import static org.mockito.Mockito.doReturn;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
public class PreprocessingServiceTest extends AbstractTestNGSpringContextTests {
    @Autowired
    private PreprocessingService preprocessingService;

    @Test
    public void testConvertIfBQLDoesNothingToJSON() throws Exception {
        String query = "{}";
        String newQuery = preprocessingService.convertIfBQL(query);
        Assert.assertEquals(query, newQuery);
    }

    @Test
    public void testConvertIfBQLConverts() throws Exception {
        String query = "SELECT * FROM STREAM(30000, TIME) LIMIT 1;";
        String newQuery = preprocessingService.convertIfBQL(query);
        Assert.assertEquals("{\"aggregation\":{\"size\":1,\"type\":\"RAW\"},\"duration\":30000}", newQuery);
    }

    @Test(expectedExceptions = BQLException.class)
    public void testConvertIfBQLThrowsIfQueryBad() throws Exception {
        String query = "garbage";
        preprocessingService.convertIfBQL(query);
    }

    @Test
    public void testContainsWindowHasWindow() {
        String query = "{\"window\":{\"emit\":{\"type\": \"TIME\",\"every\": 5000}}}";
        Assert.assertTrue(preprocessingService.containsWindow(query));
    }

    @Test
    public void testContainsWindowHasNoWindow() {
        String query = "{\"filters\":[{\"field\":\"demog_info\",\"operation\":\"!=\",\"values\":[\"null\"]}]}";
        Assert.assertFalse(preprocessingService.containsWindow(query));
    }

    @Test(expectedExceptions = JsonSyntaxException.class)
    public void testContainsWindowThrows() throws Exception {
        String query = "garbage";
        preprocessingService.containsWindow(query);
    }

    @Test(expectedExceptions = TooManyQueriesException.class)
    public void testThrowIfQueryLimitReachedTooManyQueries() throws Exception {
        ConcurrentMap<String, QueryHandler> map = Mockito.mock(ConcurrentMap.class);
        doReturn(500).when(map).size();
        QueryService queryService = Mockito.mock(QueryService.class);
        doReturn(map).when(queryService).getRunningQueries();
        preprocessingService.throwIfQueryLimitReached(queryService);
    }

    @Test
    public void testThrowIfQueryLimitReached() throws Exception {
        ConcurrentMap<String, QueryHandler> map = Mockito.mock(ConcurrentMap.class);
        doReturn(499).when(map).size();
        QueryService queryService = Mockito.mock(QueryService.class);
        doReturn(map).when(queryService).getRunningQueries();
        preprocessingService.throwIfQueryLimitReached(queryService);
    }
}
