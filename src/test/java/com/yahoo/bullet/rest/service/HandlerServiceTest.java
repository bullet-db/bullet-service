/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import com.yahoo.bullet.rest.query.QueryError;
import com.yahoo.bullet.rest.query.QueryHandler;
import lombok.NoArgsConstructor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.UUID;

import static java.util.Collections.singletonList;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class HandlerServiceTest {
    @NoArgsConstructor
    public static class MockSubscriber implements Subscriber {
        @Override
        public PubSubMessage receive() {
            return null;
        }

        @Override
        public void close() {
        }

        @Override
        public void commit(String s) {
        }

        @Override
        public void fail(String s) {
        }
    }

    @Test
    public void testClose() {
        Publisher publisher = Mockito.mock(Publisher.class);
        Subscriber subscriber = new MockSubscriber();
        QueryHandler queryHandler = Mockito.mock(QueryHandler.class);
        HandlerService service = new HandlerService(singletonList(publisher), singletonList(subscriber), 1);
        service.submit("", "{", queryHandler);
        service.close();
        verify(queryHandler).fail();
        Assert.assertTrue(service.getHandlers().isEmpty());
    }

    @Test
    public void testSubmitWhenPublisherFails() throws Exception {
        Publisher publisher = Mockito.mock(Publisher.class);
        Subscriber subscriber = new MockSubscriber();
        QueryHandler queryHandler = Mockito.mock(QueryHandler.class);
        doThrow(new PubSubException("")).when(publisher).send(anyString(), anyString());
        HandlerService service = new HandlerService(singletonList(publisher), singletonList(subscriber), 1);
        service.submit("", "", queryHandler);
        verify(queryHandler).fail(any(QueryError.class));
        service.close();
    }

    @Test
    public void testSubmit() throws Exception {
        Publisher publisher = Mockito.mock(Publisher.class);
        Subscriber subscriber = new MockSubscriber();
        QueryHandler queryHandler = Mockito.mock(QueryHandler.class);
        HandlerService service = new HandlerService(singletonList(publisher), singletonList(subscriber), 1);
        String randomID = UUID.randomUUID().toString();
        String randomContent = "{foo}";
        service.submit(randomID, randomContent, queryHandler);
        Assert.assertTrue(service.hasHandlers(randomID));
        Assert.assertEquals(1, service.count());
        Assert.assertEquals(1, service.getHandlers().size());
        Assert.assertEquals(queryHandler, service.getHandlers().get(randomID));
        Assert.assertSame(service.getHandler(randomID), queryHandler);
        Assert.assertSame(service.removeHandler(randomID), queryHandler);
        Assert.assertFalse(service.hasHandlers(randomID));
        Assert.assertEquals(0, service.count());
        Assert.assertEquals(0, service.getHandlers().size());
        verify(publisher).send(randomID, randomContent);
        service.close();
    }

    @Test
    public void testSubmitFailsFromPublisherError() throws Exception {
        Publisher publisher = Mockito.mock(Publisher.class);
        Subscriber subscriber = new MockSubscriber();
        QueryHandler queryHandler = Mockito.mock(QueryHandler.class);
        doThrow(new PubSubException("")).when(publisher).send(any(), any());
        HandlerService service = new HandlerService(singletonList(publisher), singletonList(subscriber), 1);
        String randomID = UUID.randomUUID().toString();
        String query = "{}";
        service.submit(randomID, query, queryHandler);
        verify(queryHandler).fail(QueryError.SERVICE_UNAVAILABLE);
    }

    @Test
    public void testKillQuery() throws Exception {
        Publisher publisher = Mockito.mock(Publisher.class);
        Subscriber subscriber = new MockSubscriber();
        QueryHandler queryHandler = Mockito.mock(QueryHandler.class);
        HandlerService service = new HandlerService(singletonList(publisher), singletonList(subscriber), 1);
        service.getHandlers().put("id", queryHandler);

        QueryHandler actualHandler = service.killQuery("id");
        Assert.assertSame(actualHandler, queryHandler);
        Assert.assertEquals(0, service.count());
        Assert.assertEquals(0, service.getHandlers().size());
        verify(publisher).send(new PubSubMessage("id", Metadata.Signal.KILL));
        service.close();
    }

    @Test
    public void testKillQueryWhenPublisherFails() throws Exception {
        Publisher publisher = Mockito.mock(Publisher.class);
        Subscriber subscriber = new MockSubscriber();
        QueryHandler queryHandler = Mockito.mock(QueryHandler.class);
        HandlerService service = new HandlerService(singletonList(publisher), singletonList(subscriber), 1);
        service.getHandlers().put("id", queryHandler);

        doThrow(new PubSubException("")).when(publisher).send(any());
        service.killQuery("id");
        Assert.assertEquals(0, service.getHandlers().size());
        service.close();
    }

    @Test
    public void testFailQuery() {
        Publisher publisher = Mockito.mock(Publisher.class);
        Subscriber subscriber = new MockSubscriber();
        QueryHandler queryHandler = Mockito.mock(QueryHandler.class);
        HandlerService service = new HandlerService(singletonList(publisher), singletonList(subscriber), 1);

        Assert.assertFalse(service.failHandler("id"));
        verifyZeroInteractions(queryHandler);
        service.getHandlers().put("id", queryHandler);
        Assert.assertTrue(service.failHandler("id"));
        verify(queryHandler).fail();
    }

    @Test
    public void testQueryCount() {
        Publisher publisher = Mockito.mock(Publisher.class);
        Subscriber subscriber = new MockSubscriber();
        QueryHandler queryHandler = Mockito.mock(QueryHandler.class);
        HandlerService service = new HandlerService(singletonList(publisher), singletonList(subscriber), 1);

        Assert.assertEquals(service.count(), 0);
        service.getHandlers().put("id", queryHandler);
        Assert.assertEquals(service.count(), 1);
    }

    @Test
    public void testSendingSignalForQuery() throws Exception {
        Publisher publisher = Mockito.mock(Publisher.class);
        Subscriber subscriber = new MockSubscriber();
        HandlerService service = new HandlerService(singletonList(publisher), singletonList(subscriber), 1);

        service.sendSignal("id", Metadata.Signal.COMPLETE);
        verify(publisher).send(new PubSubMessage("id", Metadata.Signal.COMPLETE));
    }

    @Test
    public void testSendingMessage() throws Exception {
        Publisher publisher = Mockito.mock(Publisher.class);
        Subscriber subscriber = new MockSubscriber();
        HandlerService service = new HandlerService(singletonList(publisher), singletonList(subscriber), 1);

        service.sendMessage(new PubSubMessage("foo", Metadata.Signal.ACKNOWLEDGE));
        verify(publisher).send(new PubSubMessage("foo", Metadata.Signal.ACKNOWLEDGE));
    }
}
