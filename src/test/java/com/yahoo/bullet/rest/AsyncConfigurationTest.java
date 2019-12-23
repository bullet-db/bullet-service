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
import java.util.Collections;
import java.util.List;

public class AsyncConfigurationTest {
    @Test
    public void testResponderClasses() {
        List<String> classes = Arrays.asList("com.yahoo.bullet.pubsub.BulletPubSubResponder", "com.yahoo.bullet.pubsub.BulletPubSubResponder");
        BulletConfig config = new BulletConfig("async_defaults.yaml");

        ResponderClasses responderClassesA = new ResponderClasses();
        responderClassesA.setClasses(classes);
        List<PubSubResponder> respondersA = responderClassesA.create(config);
        Assert.assertEquals(respondersA.size(), 2);
        Assert.assertTrue(respondersA.get(0) instanceof BulletPubSubResponder);
        Assert.assertTrue(respondersA.get(1) instanceof BulletPubSubResponder);

        ResponderClasses responderClassesB = new ResponderClasses(classes);
        Assert.assertEquals(responderClassesA.getClasses(), responderClassesB.getClasses());
        List<PubSubResponder> respondersB = responderClassesB.create(config);
        Assert.assertEquals(respondersB.size(), 2);
        Assert.assertTrue(respondersB.get(0) instanceof BulletPubSubResponder);
        Assert.assertTrue(respondersA.get(1) instanceof BulletPubSubResponder);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testResponderClassesMissingClass() {
        List<String> classes = Arrays.asList("does.not.exist", "com.yahoo.bullet.pubsub.BulletPubSubResponder");
        ResponderClasses responderClasses = new ResponderClasses(classes);
        responderClasses.create(new BulletConfig("async_defaults.yaml"));
    }

    @Test
    public void testCreatingAsyncRespondersWithNoClasses() {
        AsyncConfiguration configuration = new AsyncConfiguration();
        Assert.assertTrue(configuration.asyncResponders(null, null).isEmpty());
    }

    @Test
    public void testCreatingAsyncResponders() {
        AsyncConfiguration configuration = new AsyncConfiguration();
        BulletConfig config = configuration.asyncConfig("async_defaults.yaml");
        ResponderClasses classes = configuration.responderClasses();
        classes.setClasses(Collections.singletonList("com.yahoo.bullet.pubsub.BulletPubSubResponder"));

        List<PubSubResponder> responders = configuration.asyncResponders(config, classes);
        Assert.assertEquals(responders.size(), 1);
        Assert.assertTrue(responders.get(0) instanceof BulletPubSubResponder);
    }
}
