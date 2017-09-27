/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.schema;


import org.testng.Assert;
import org.testng.annotations.Test;

public class EnumerationTest {
    @Test
    public void testNoDataConstructor() {
        Enumeration enumeration = new Enumeration();
        Assert.assertNull(enumeration.getName());
        Assert.assertNull(enumeration.getDescription());
    }

    @Test
    public void testSettersWithData() {
        Enumeration enumeration = new Enumeration();
        enumeration.setDescription("foo");
        enumeration.setName("bar");
        Assert.assertEquals(enumeration.getDescription(), "foo");
        Assert.assertEquals(enumeration.getName(), "bar");
    }
}
