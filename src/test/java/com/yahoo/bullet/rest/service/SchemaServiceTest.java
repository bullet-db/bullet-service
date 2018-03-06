/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.google.gson.GsonBuilder;
import com.yahoo.bullet.rest.schema.JSONAPIColumn;
import com.yahoo.bullet.rest.schema.JSONAPIDocument;
import com.yahoo.bullet.typesystem.Type;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
public class SchemaServiceTest extends AbstractTestNGSpringContextTests {
    @Autowired
    SchemaService service;

    @Test
    public void testClasspathResource() {
        Assert.assertNotNull(service);
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
        Assert.assertEquals(expected, service.getSchema());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testBadFile() {
        new SchemaService("0.1", "/does/not/exist");
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testBadType() {
        new SchemaService("0.1", "src/test/resources/bad_type.json");
    }

    @Test
    public void testAllTypesAndOperators() {
        SchemaService testService = new SchemaService("0.1", "src/test/resources/all_types.json");
        String json = testService.getSchema();
        JSONAPIDocument document = new GsonBuilder().create().fromJson(json, JSONAPIDocument.class);

        List<JSONAPIColumn> columns = document.getData();
        Assert.assertEquals(columns.size(), 7);

        Assert.assertEquals(columns.get(0).getId(), "fake_A");
        Assert.assertEquals(columns.get(0).getType(), "column");
        Assert.assertEquals(columns.get(0).getAttributes().getName(), "fake_A");
        Assert.assertEquals(columns.get(0).getAttributes().getType(), Type.DOUBLE);

        Assert.assertEquals(columns.get(1).getId(), "fake_B");
        Assert.assertEquals(columns.get(1).getType(), "column");
        Assert.assertEquals(columns.get(1).getAttributes().getName(), "fake_B");
        Assert.assertEquals(columns.get(1).getAttributes().getType(), Type.STRING);
        Assert.assertNull(columns.get(1).getAttributes().getSubtype());
        Assert.assertEquals(columns.get(1).getAttributes().getDescription(), "fake column B. Contains strings");
        Assert.assertNull(columns.get(1).getAttributes().getEnumerations());

        Assert.assertEquals(columns.get(2).getId(), "fake_C");
        Assert.assertEquals(columns.get(2).getType(), "column");
        Assert.assertEquals(columns.get(2).getAttributes().getName(), "fake_C");
        Assert.assertEquals(columns.get(2).getAttributes().getType(), Type.BOOLEAN);

        Assert.assertEquals(columns.get(3).getId(), "fake_D");
        Assert.assertEquals(columns.get(3).getType(), "column");
        Assert.assertEquals(columns.get(3).getAttributes().getName(), "fake_D");
        Assert.assertEquals(columns.get(3).getAttributes().getType(), Type.LONG);

        Assert.assertEquals(columns.get(4).getId(), "fake_E");
        Assert.assertEquals(columns.get(4).getType(), "column");
        Assert.assertEquals(columns.get(4).getAttributes().getName(), "fake_E");
        Assert.assertEquals(columns.get(4).getAttributes().getType(), Type.MAP);
        Assert.assertEquals(columns.get(4).getAttributes().getSubtype(), Type.STRING);
        Assert.assertNotNull(columns.get(4).getAttributes().getEnumerations());
        Assert.assertEquals(columns.get(4).getAttributes().getEnumerations().size(), 2);
        Assert.assertEquals(columns.get(4).getAttributes().getEnumerations().get(0).getName(), "subfield_A");
        Assert.assertEquals(columns.get(4).getAttributes().getEnumerations().get(1).getName(), "subfield_B");
        Assert.assertEquals(columns.get(4).getAttributes().getEnumerations().get(1).getDescription(),
                            "description for the subfield B in fake_E");

        Assert.assertEquals(columns.get(5).getId(), "fake_F");
        Assert.assertEquals(columns.get(5).getType(), "column");
        Assert.assertEquals(columns.get(5).getAttributes().getName(), "fake_F");
        Assert.assertEquals(columns.get(5).getAttributes().getType(), Type.LIST);
        Assert.assertEquals(columns.get(5).getAttributes().getSubtype(), Type.MAP);
        Assert.assertNull(columns.get(5).getAttributes().getEnumerations());

        Assert.assertEquals(columns.get(6).getId(), "fake_G");
        Assert.assertEquals(columns.get(6).getType(), "column");
        Assert.assertEquals(columns.get(6).getAttributes().getName(), "fake_G");
        Assert.assertEquals(columns.get(6).getAttributes().getType(), Type.MAP);
        Assert.assertEquals(columns.get(6).getAttributes().getSubtype(), Type.BOOLEAN);
        Assert.assertNull(columns.get(6).getAttributes().getEnumerations());
    }
}
