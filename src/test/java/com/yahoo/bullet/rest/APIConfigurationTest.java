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
import com.yahoo.bullet.rest.query.MockPubSub;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
public class APIConfigurationTest extends AbstractTestNGSpringContextTests {
    @Autowired
    private WebMvcConfigurer corsConfigurer;
    @Autowired
    private PubSub pubSub;
    @Autowired
    private List<Publisher> publishers;
    @Autowired
    private List<Subscriber> subscribers;

    @Test
    public void testCORSConfiguration() {
        Assert.assertNotNull(corsConfigurer);
        CorsRegistry mockRegistry = mock(CorsRegistry.class);
        corsConfigurer.addCorsMappings(mockRegistry);
        verify(mockRegistry).addMapping(eq("/**"));
    }

    @Test
    public void testPubSub() {
        Assert.assertNotNull(pubSub);
        Assert.assertTrue(pubSub instanceof MockPubSub);

        Assert.assertNotNull(publishers);
        Assert.assertEquals(publishers.size(), 0);

        Assert.assertNotNull(subscribers);
        Assert.assertEquals(subscribers.size(), 0);

        MockPubSub mockPubSub = (MockPubSub) pubSub;
        Assert.assertEquals(mockPubSub.getConfig().get(BulletConfig.PUBSUB_CONTEXT_NAME),
                            PubSub.Context.QUERY_SUBMISSION.name());

        Assert.assertEquals(mockPubSub.getPublishersAskedFor().size(), 1);
        Assert.assertEquals(mockPubSub.getPublishersAskedFor().get(0), Integer.valueOf(1));

        Assert.assertEquals(mockPubSub.getSubscribersAskedFor().size(), 1);
        Assert.assertEquals(mockPubSub.getSubscribersAskedFor().get(0), Integer.valueOf(1));

    }
}
