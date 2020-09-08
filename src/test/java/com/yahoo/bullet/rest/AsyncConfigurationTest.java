/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.BulletPubSubResponder;
import com.yahoo.bullet.pubsub.PubSubResponder;
import com.yahoo.bullet.rest.AsyncConfiguration.ResponderClasses;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

public class AsyncConfigurationTest {
    @Test
    public void testResponderClasses() {
        List<String> classes = Arrays.asList("com.yahoo.bullet.pubsub.BulletPubSubResponder", "com.yahoo.bullet.pubsub.BulletPubSubResponder");
        BulletConfig config = new BulletConfig("async_defaults.yaml");

        ResponderClasses responderClasses = new ResponderClasses(config);
        responderClasses.setClasses(classes);
        List<PubSubResponder> responders = responderClasses.create();
        Assert.assertEquals(responders.size(), 2);
        Assert.assertTrue(responders.get(0) instanceof BulletPubSubResponder);
        Assert.assertTrue(responders.get(1) instanceof BulletPubSubResponder);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testResponderClassesMissingClass() {
        List<String> classes = Arrays.asList("does.not.exist", "com.yahoo.bullet.pubsub.BulletPubSubResponder");
        ResponderClasses responderClasses = new ResponderClasses(new BulletConfig("async_defaults.yaml"));
        responderClasses.setClasses(classes);
        responderClasses.create();
    }
}
