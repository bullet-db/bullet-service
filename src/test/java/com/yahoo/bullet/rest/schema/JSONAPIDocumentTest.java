/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.schema;

import org.junit.Test;
import org.testng.Assert;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class JSONAPIDocumentTest {
    @Test
    public void testNoArgsConstructorInjectsNulls() {
        JSONAPIDocument jsonapiDocument = new JSONAPIDocument();
        Assert.assertNull(jsonapiDocument.getData());
        Assert.assertNull(jsonapiDocument.getMeta());
    }

    @Test
    public void testConstructorInjectsDataAndMeta() {
        Map<String, Object> meta = new HashMap<>();
        meta.put("foo", "bar");
        JSONAPIDocument jsonapiDocument = new JSONAPIDocument(Collections.EMPTY_LIST, meta);
        Assert.assertEquals(Collections.EMPTY_LIST, jsonapiDocument.getData());
        Assert.assertEquals(meta, jsonapiDocument.getMeta());
    }

    @Test
    public void testSetterInjectsDataAndMeta() {
        Map<String, Object> meta = new HashMap<>();
        meta.put("foo", "bar");
        JSONAPIDocument jsonapiDocument = new JSONAPIDocument();
        jsonapiDocument.setMeta(meta);
        Assert.assertEquals(meta, jsonapiDocument.getMeta());
        Assert.assertNull(jsonapiDocument.getData());
    }
}
