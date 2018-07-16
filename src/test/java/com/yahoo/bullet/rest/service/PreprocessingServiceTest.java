/*
 *  Copyright 2018 Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.rest.query.HTTPQueryHandler;
import com.yahoo.bullet.rest.query.QueryHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;
import org.testng.Assert;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
public class PreprocessingServiceTest extends AbstractTestNGSpringContextTests {
    @Autowired
    private PreprocessingService preprocessingService;

    @Test
    public void testConvertIfBQLDoesNothingToJSON() {
        String query = "{}";
        QueryHandler queryHandler = new HTTPQueryHandler();
        String newQuery = preprocessingService.convertIfBQL(query, queryHandler);
        Assert.assertEquals(query, newQuery);
        Assert.assertFalse(queryHandler.isComplete());
    }

    @Test
    public void testConvertIfBQLConverts() {
        String query = "SELECT * FROM STREAM(30000, TIME) LIMIT 1;";
        QueryHandler queryHandler = new HTTPQueryHandler();
        String newQuery = preprocessingService.convertIfBQL(query, queryHandler);
        Assert.assertEquals("{\"aggregation\":{\"size\":1,\"type\":\"RAW\"},\"duration\":30000}", newQuery);
        Assert.assertFalse(queryHandler.isComplete());
    }

    @Test
    public void testConvertIfBQLCompletesIfBadQuery() {
        String query = "garbage";
        QueryHandler queryHandler = new HTTPQueryHandler();
        String newQuery = preprocessingService.convertIfBQL(query, queryHandler);
        Assert.assertNull(newQuery);
        Assert.assertTrue(queryHandler.isComplete());
    }

    @Test
    public void testFailIfWindowedFailsWindows() {
        String query = "{\"window\":{\"emit\":{\"type\": \"TIME\",\"every\": 5000}}}";
        QueryHandler queryHandler = new HTTPQueryHandler();
        preprocessingService.failIfWindowed(query, queryHandler);
        Assert.assertTrue(queryHandler.isComplete());
    }

    @Test
    public void testFailIfWindowedFailsBQLWindows() {
        String query = "SELECT COUNT(*) AS numSeniors FROM STREAM(20000, TIME) GROUP BY () WINDOWING(EVERY, 2000, TIME, ALL);";
        QueryHandler queryHandler = new HTTPQueryHandler();
        preprocessingService.failIfWindowed(query, queryHandler);
        Assert.assertTrue(queryHandler.isComplete());
    }

    @Test
    public void testFailIfWindowedGoodQuery() {
        String query = "{\"filters\":[{\"field\":\"demog_info\",\"operation\":\"!=\",\"values\":[\"null\"]}]}";
        QueryHandler queryHandler = new HTTPQueryHandler();
        preprocessingService.failIfWindowed(query, queryHandler);
        Assert.assertFalse(queryHandler.isComplete());
    }

    @Test
    public void testFailIfWindowsdGoodBQLQuery() {
        String query = "SELECT COUNT(*) AS numSeniors FROM STREAM(20000, TIME) GROUP BY ();";
        QueryHandler queryHandler = new HTTPQueryHandler();
        preprocessingService.failIfWindowed(query, queryHandler);
        Assert.assertFalse(queryHandler.isComplete());
    }

    @Test
    public void testFailIfWindowsdBadJSON() {
        String query = "{";
        QueryHandler queryHandler = new HTTPQueryHandler();
        preprocessingService.failIfWindowed(query, queryHandler);
        Assert.assertTrue(queryHandler.isComplete());
    }
}
