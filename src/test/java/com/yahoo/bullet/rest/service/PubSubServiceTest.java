/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import com.yahoo.bullet.rest.resource.QueryError;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.UUID;

import static java.util.Collections.singletonList;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
public class PubSubServiceTest {
    @Test
    public void testClose() throws Exception {
        Publisher publisher = Mockito.mock(Publisher.class);
        Subscriber subscriber = Mockito.mock(Subscriber.class);
        QueryHandler queryHandler = Mockito.mock(QueryHandler.class);
        PubSub pubSub = Mockito.mock(PubSub.class);
        Mockito.when(pubSub.getPublishers(anyInt())).thenReturn(singletonList(publisher));
        Mockito.when(pubSub.getSubscribers(anyInt())).thenReturn(singletonList(subscriber));
        PubSubService service = new PubSubService(pubSub, 1, 1, 1);
        service.submit("", "", queryHandler);
        service.close();
        Mockito.verify(queryHandler).fail();
        Assert.assertTrue(service.getRequestQueue().isEmpty());
    }

    @Test
    public void testSubmitWhenPublisherFails() throws Exception {
        Publisher publisher = Mockito.mock(Publisher.class);
        Subscriber subscriber = Mockito.mock(Subscriber.class);
        QueryHandler queryHandler = Mockito.mock(QueryHandler.class);
        doThrow(new PubSubException("")).when(publisher).send(anyString(), anyString());
        PubSub pubSub = Mockito.mock(PubSub.class);
        Mockito.when(pubSub.getPublishers(anyInt())).thenReturn(singletonList(publisher));
        Mockito.when(pubSub.getSubscribers(anyInt())).thenReturn(singletonList(subscriber));
        PubSubService service = new PubSubService(pubSub, 1, 1, 1);
        service.submit("", "", queryHandler);
        Mockito.verify(queryHandler).fail(any(QueryError.class));
        service.close();
    }

    @Test
    public void testSubmit() throws Exception {
        Publisher publisher = Mockito.mock(Publisher.class);
        Subscriber subscriber = Mockito.mock(Subscriber.class);
        QueryHandler queryHandler = Mockito.mock(QueryHandler.class);
        PubSub pubSub = Mockito.mock(PubSub.class);
        Mockito.when(pubSub.getPublishers(anyInt())).thenReturn(singletonList(publisher));
        Mockito.when(pubSub.getSubscribers(anyInt())).thenReturn(singletonList(subscriber));
        PubSubService service = new PubSubService(pubSub, 1, 1, 1);
        String randomID = UUID.randomUUID().toString();
        String randomContent = "foo";
        service.submit(randomID, randomContent, queryHandler);
        Assert.assertEquals(service.getRequestQueue().size(), 1);
        Assert.assertEquals(service.getRequestQueue().get(randomID), queryHandler);
        Mockito.verify(publisher).send(randomID, randomContent);
        service.close();
    }
}
