/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.rest.query.QueryHandler;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

public class HandlerServiceTest {
    @Test
    public void testAddRemoveHandler() {
        QueryHandler queryHandler = mock(QueryHandler.class);
        HandlerService service = new HandlerService();
        String randomID = UUID.randomUUID().toString();
        service.addHandler(randomID, queryHandler);
        Assert.assertTrue(service.hasHandler(randomID));
        Assert.assertEquals(service.count(), 1);
        Assert.assertEquals(service.getHandlers().size(), 1);
        Assert.assertEquals(service.getHandlers().get(randomID), queryHandler);
        Assert.assertSame(service.getHandler(randomID), queryHandler);
        Assert.assertSame(service.removeHandler(randomID), queryHandler);
        Assert.assertFalse(service.hasHandler(randomID));
        Assert.assertEquals(service.count(), 0);
        Assert.assertEquals(service.getHandlers().size(), 0);
    }

    @Test
    public void testFailHandler() {
        QueryHandler queryHandler = mock(QueryHandler.class);
        HandlerService service = new HandlerService();

        Assert.assertFalse(service.failHandler("id"));
        verifyZeroInteractions(queryHandler);
        service.getHandlers().put("id", queryHandler);
        Assert.assertTrue(service.failHandler("id"));
        verify(queryHandler).fail();
        Assert.assertFalse(service.failHandler("id"));
        verifyNoMoreInteractions(queryHandler);
    }

    @Test
    public void testQueryCount() {
        QueryHandler queryHandler = mock(QueryHandler.class);
        HandlerService service = new HandlerService();

        Assert.assertEquals(service.count(), 0);
        service.getHandlers().put("id1", queryHandler);
        Assert.assertEquals(service.count(), 1);
        service.getHandlers().put("id2", queryHandler);
        Assert.assertEquals(service.count(), 2);
        service.removeHandler("id1");
        Assert.assertEquals(service.count(), 1);
        service.removeHandler("id2");
        Assert.assertEquals(service.count(), 0);
    }

    @Test
    public void testFailingAll() {
        QueryHandler queryHandlerA = mock(QueryHandler.class);
        QueryHandler queryHandlerB = mock(QueryHandler.class);
        HandlerService service = new HandlerService();
        service.addHandler("A", queryHandlerA);
        service.addHandler("B", queryHandlerB);
        verifyZeroInteractions(queryHandlerA);
        verifyZeroInteractions(queryHandlerB);

        service.failAllHandlers();
        verify(queryHandlerA).fail();
        verify(queryHandlerB).fail();
        Assert.assertEquals(service.count(), 0);
    }

    @Test
    public void testClose() {
        QueryHandler queryHandler = mock(QueryHandler.class);
        HandlerService service = new HandlerService();
        service.addHandler("", queryHandler);
        service.close();
        verify(queryHandler).fail();
        Assert.assertTrue(service.getHandlers().isEmpty());
    }

    @Test
    public void testRespondingToACompletedHandler() {
        QueryHandler handler = mock(QueryHandler.class);
        doReturn(true).when(handler).isComplete();
        HandlerService service = new HandlerService();
        service.addHandler("id", handler);

        PubSubMessage message = new PubSubMessage("id", Metadata.Signal.FAIL);
        service.respond("id", message);
        verify(handler).isComplete();
        verifyNoMoreInteractions(handler);
    }

    @Test
    public void testRespondingToAIncompleteHandlerWithAMessage() {
        QueryHandler handler = mock(QueryHandler.class);
        doReturn(false).when(handler).isComplete();
        HandlerService service = new HandlerService();
        service.addHandler("id", handler);

        PubSubMessage message = new PubSubMessage("id", "content");
        service.respond("id", message);
        verify(handler, times(2)).isComplete();
        verify(handler).send(eq(message));
        verifyNoMoreInteractions(handler);
        Assert.assertTrue(service.hasHandler("id"));
    }

    @Test
    public void testRespondingToAIncompleteHandlerWithACompleteMessage() {
        QueryHandler handler = mock(QueryHandler.class);
        doReturn(false).doReturn(true).when(handler).isComplete();
        HandlerService service = new HandlerService();
        service.addHandler("id", handler);

        PubSubMessage message = new PubSubMessage("id", Metadata.Signal.FAIL);
        service.respond("id", message);
        verify(handler).send(eq(message));
        verify(handler).complete();
        Assert.assertFalse(service.hasHandler("id"));
    }
}
