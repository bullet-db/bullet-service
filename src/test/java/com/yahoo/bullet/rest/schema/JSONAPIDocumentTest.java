/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.schema;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class JSONAPIDocumentTest {
    @Test
    public void testNoDataConstructor() {
        JSONAPIDocument jsonapiDocument = new JSONAPIDocument();
        Assert.assertNull(jsonapiDocument.getData());
        Assert.assertNull(jsonapiDocument.getMeta());
    }

    @Test
    public void testConstructorWithData() {
        Map<String, Object> meta = new HashMap<>();
        meta.put("foo", "bar");
        JSONAPIDocument jsonapiDocument = new JSONAPIDocument(Collections.emptyList(), meta);
        Assert.assertEquals(jsonapiDocument.getData(), Collections.EMPTY_LIST);
        Assert.assertEquals(jsonapiDocument.getMeta(), meta);
    }

    @Test
    public void testSettingData() {
        Map<String, Object> meta = new HashMap<>();
        meta.put("foo", "bar");
        JSONAPIDocument jsonapiDocument = new JSONAPIDocument();
        jsonapiDocument.setMeta(meta);
        Assert.assertEquals(jsonapiDocument.getMeta(), meta);
        Assert.assertNull(jsonapiDocument.getData());
    }
}
