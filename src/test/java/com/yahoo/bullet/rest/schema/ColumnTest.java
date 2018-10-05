/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.schema;

import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;

public class ColumnTest {
    private Column column;

    @BeforeMethod
    public void setup() {
        column = new Column();
        column.setName("foo");
        column.setDescription("bar");
    }

    @Test
    public void testNameValidation() {
        column.setName(null);
        column.setType(Type.STRING);
        Assert.assertFalse(column.isValid());
        column.setName("");
        Assert.assertFalse(column.isValid());
        column.setName("foo");
        Assert.assertTrue(column.isValid());
    }

    @Test
    public void testTypeValidation() {
        column.setType(null);
        Assert.assertFalse(column.isValid());
        column.setType(Type.BOOLEAN);
        Assert.assertTrue(column.isValid());
    }

    @Test
    public void testSubtypeValidation() {
        // Subtype is only valid for non primitives
        column.setType(Type.STRING);
        column.setSubtype(Type.BOOLEAN);
        Assert.assertFalse(column.isValid());

        // Subtype must be set if type is not a primitive
        column.setType(Type.MAP);
        column.setSubtype(null);
        Assert.assertFalse(column.isValid());
        column.setType(Type.LIST);
        Assert.assertFalse(column.isValid());

        // Subtype can only be primitives or MAP if type is MAP
        column.setType(Type.MAP);
        column.setSubtype(Type.MAP);
        Assert.assertTrue(column.isValid());
        column.setSubtype(Type.LIST);
        Assert.assertFalse(column.isValid());
        column.setSubtype(Type.BOOLEAN);
        Assert.assertTrue(column.isValid());

        // Subtype can only be primitives or MAP if type is LIST
        column.setType(Type.LIST);
        column.setSubtype(Type.MAP);
        Assert.assertTrue(column.isValid());
        column.setSubtype(Type.LIST);
        Assert.assertFalse(column.isValid());
        column.setSubtype(Type.BOOLEAN);
        Assert.assertTrue(column.isValid());
    }

    @Test
    public void testEnumerations() {
        column.setType(Type.STRING);
        column.setEnumerations(Collections.emptyList());
        Assert.assertFalse(column.isValid());

        column.setType(Type.MAP);
        column.setSubtype(Type.STRING);
        column.setEnumerations(Collections.emptyList());
        Assert.assertTrue(column.isValid());
    }
}
