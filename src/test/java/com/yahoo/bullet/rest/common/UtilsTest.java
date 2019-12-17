/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.common;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.Metadata.Signal;
import com.yahoo.bullet.pubsub.PubSubMessage;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class UtilsTest {
    @Test
    public void testIDsRandom() {
        Set<String> ids = IntStream.range(0, 100).mapToObj(i -> Utils.getNewQueryID()).collect(Collectors.toSet());
        Assert.assertEquals(ids.size(), 100);
    }

    @Test
    public void testDonePubSubMessage() {
        Assert.assertFalse(Utils.isDone(new PubSubMessage("id", "")));
        Assert.assertFalse(Utils.isDone(new PubSubMessage("id", (byte[]) null, Signal.CUSTOM)));
        Assert.assertFalse(Utils.isDone(new PubSubMessage("id", (byte[]) null, new Metadata(Signal.REPLAY, null))));
        Assert.assertFalse(Utils.isDone(new PubSubMessage("id", (byte[]) null, new Metadata(Signal.ACKNOWLEDGE, null))));
        Assert.assertFalse(Utils.isDone(new PubSubMessage("id", (byte[]) null, new Metadata(Signal.CUSTOM, null))));
        Assert.assertTrue(Utils.isDone(new PubSubMessage("id", (byte[]) null, new Metadata(Signal.KILL, null))));
        Assert.assertTrue(Utils.isDone(new PubSubMessage("id", (byte[]) null, new Metadata(Signal.FAIL, null))));
        Assert.assertTrue(Utils.isDone(new PubSubMessage("id", (byte[]) null, new Metadata(Signal.COMPLETE, null))));
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testNull() {
        Utils.checkNotEmpty(null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testEmpty() {
        Utils.checkNotEmpty(Collections.emptyList());
    }

    @Test
    public void testNotEmpty() {
        Utils.checkNotEmpty(Collections.singleton("foo"));
        Utils.checkNotEmpty(Collections.singletonList("foo"));
        Assert.assertTrue(true);
    }
}
