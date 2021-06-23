/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.IdentityPubSubMessageSerDe;
import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubMessageSerDe;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import com.yahoo.bullet.rest.common.MockPubSub;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

public class PubSubConfigurationTest {
    @Test
    public void testPubSub() throws Exception {
        PubSubConfiguration configuration = new PubSubConfiguration();
        BulletConfig config = configuration.pubSubConfig("test_pubsub_defaults.yaml");
        PubSub pubSub = configuration.pubSub(config);

        Assert.assertNotNull(pubSub);
        Assert.assertTrue(pubSub instanceof MockPubSub);
        MockPubSub mockPubSub = (MockPubSub) pubSub;
        Assert.assertEquals(mockPubSub.getConfig().get(BulletConfig.PUBSUB_CONTEXT_NAME), PubSub.Context.QUERY_SUBMISSION.name());

        List<Publisher> publishers = configuration.publishers(pubSub, 1);
        List<Subscriber> subscribers = configuration.subscribers(pubSub, 1);

        Assert.assertNotNull(publishers);
        Assert.assertEquals(publishers.size(), 1);

        Assert.assertNotNull(subscribers);
        Assert.assertEquals(subscribers.size(), 1);

        Assert.assertEquals(mockPubSub.getPublishersAskedFor().size(), 1);
        Assert.assertEquals(mockPubSub.getPublishersAskedFor().get(0), Integer.valueOf(1));

        Assert.assertEquals(mockPubSub.getSubscribersAskedFor().size(), 1);
        Assert.assertEquals(mockPubSub.getSubscribersAskedFor().get(0), Integer.valueOf(1));
    }

    @Test
    public void testSerDe() {
        PubSubConfiguration configuration = new PubSubConfiguration();
        BulletConfig config = configuration.pubSubConfig("test_pubsub_defaults.yaml");

        PubSubMessageSerDe serDe = configuration.pubSubMessageSendSerDe(config);
        Assert.assertTrue(serDe instanceof IdentityPubSubMessageSerDe);
    }
}
