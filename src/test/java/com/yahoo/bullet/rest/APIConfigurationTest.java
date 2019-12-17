/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import com.yahoo.bullet.rest.common.MockPubSub;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class APIConfigurationTest {
    @Test
    public void testCORSConfiguration() {
        APIConfiguration configuration = new APIConfiguration();
        WebMvcConfigurer corsConfigurer = configuration.corsConfigurer();
        CorsRegistry mockRegistry = mock(CorsRegistry.class);
        corsConfigurer.addCorsMappings(mockRegistry);
        verify(mockRegistry).addMapping(eq("/**"));
    }

    @Test
    public void testPubSub() throws Exception {
        APIConfiguration configuration = new APIConfiguration();
        PubSub pubSub = configuration.pubSub("src/test/resources/test_pubsub_defaults.yaml");

        Assert.assertNotNull(pubSub);
        Assert.assertTrue(pubSub instanceof MockPubSub);
        MockPubSub mockPubSub = (MockPubSub) pubSub;
        Assert.assertEquals(mockPubSub.getConfig().get(BulletConfig.PUBSUB_CONTEXT_NAME), PubSub.Context.QUERY_SUBMISSION.name());

        List<Publisher> publishers = configuration.publishers(pubSub, 1);
        List<Subscriber> subscribers = configuration.subscribers(pubSub, 1);

        Assert.assertNotNull(publishers);
        Assert.assertEquals(publishers.size(), 0);

        Assert.assertNotNull(subscribers);
        Assert.assertEquals(subscribers.size(), 0);

        Assert.assertEquals(mockPubSub.getPublishersAskedFor().size(), 1);
        Assert.assertEquals(mockPubSub.getPublishersAskedFor().get(0), Integer.valueOf(1));

        Assert.assertEquals(mockPubSub.getSubscribersAskedFor().size(), 1);
        Assert.assertEquals(mockPubSub.getSubscribersAskedFor().get(0), Integer.valueOf(1));
    }
}
