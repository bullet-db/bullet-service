/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.google.gson.GsonBuilder;
import com.yahoo.bullet.rest.model.JSONAPIField;
import com.yahoo.bullet.typesystem.Schema;
import com.yahoo.bullet.typesystem.Schema.Field;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

import static com.yahoo.bullet.TestHelpers.assertJSONEquals;

public class SchemaServiceTest {
    @Test
    public void testClasspathResource() {
        String expected =
            "{'data': [{'id':'test','type':'field'," +
                       "'attributes':{'name':'test','type':'STRING_MAP','description':'foo'}}]," +
             "'meta':{'version':'1.2'}}";
        SchemaService service = new SchemaService("1.2", "test_fields.json");
        assertJSONEquals(service.getSchema(), expected);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testBadFile() {
        new SchemaService("0.1", "/does/not/exist");
    }

    @Test
    public void testLoadingComplexTypedFields() {
        List<Field> fields = new Schema("sample_fields.json").getFields();
        Assert.assertEquals(fields.size(), 16);
        List<JSONAPIField> jsonAPIFields = fields.stream().map(JSONAPIField::from).collect(Collectors.toList());
        String jsonFields = new GsonBuilder().create().toJson(jsonAPIFields);
        String expected = "{'data':" + jsonFields + ", 'meta':{'version':'0.1'}}";

        SchemaService testService = new SchemaService("0.1", "sample_fields.json");
        assertJSONEquals(testService.getSchema(), expected);
    }
}
