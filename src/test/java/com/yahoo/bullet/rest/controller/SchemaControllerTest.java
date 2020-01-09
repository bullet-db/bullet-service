/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.rest.service.SchemaService;
import org.testng.annotations.Test;

import static com.yahoo.bullet.TestHelpers.assertJSONEquals;

public class SchemaControllerTest {
    @Test
    public void testDefaultResponse() {
        SchemaService service = new SchemaService("1.2", "/test_columns.json");
        SchemaController controller = new SchemaController(service);
        String actual = controller.getJSONSchema();

        String expected =
            "{'data': [{'id':'test','type':'column'," +
                       "'attributes':{'name':'test','type':'MAP','subtype':'STRING','description':'foo'}}]," +
             "'meta':{'version':'1.2'}}";
        assertJSONEquals(actual, expected);
    }
}
