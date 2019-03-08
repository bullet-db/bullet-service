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

public class QueryServiceTest {
    @NoArgsConstructor
    public static class MockSubscriber implements Subscriber {
        @Override
        public PubSubMessage receive() throws PubSubException {
            return null;
        }

        @Override
        public void close() {
        }

        @Override
        public void commit(String s, int i) {
        }

        @Override
        public void fail(String s, int i) {
        }
    }

    @Test
    public void testClose() {
        Publisher publisher = Mockito.mock(Publisher.class);
        Subscriber subscriber = new MockSubscriber();
        QueryHandler queryHandler = Mockito.mock(QueryHandler.class);
        QueryService service = new QueryService(singletonList(publisher), singletonList(subscriber), 1);
        service.submit("", "{", queryHandler);
        service.close();
        verify(queryHandler).fail();
        Assert.assertTrue(service.getRunningQueries().isEmpty());
    }

    @Test
    public void testSubmitWhenPublisherFails() throws Exception {
        Publisher publisher = Mockito.mock(Publisher.class);
        Subscriber subscriber = new MockSubscriber();
        QueryHandler queryHandler = Mockito.mock(QueryHandler.class);
        doThrow(new PubSubException("")).when(publisher).send(anyString(), anyString());
        QueryService service = new QueryService(singletonList(publisher), singletonList(subscriber), 1);
        service.submit("", "", queryHandler);
        verify(queryHandler).fail(any(QueryError.class));
        service.close();
    }

    @Test
    public void testSubmit() throws Exception {
        Publisher publisher = Mockito.mock(Publisher.class);
        Subscriber subscriber = new MockSubscriber();
        QueryHandler queryHandler = Mockito.mock(QueryHandler.class);
        QueryService service = new QueryService(singletonList(publisher), singletonList(subscriber), 1);
        String randomID = UUID.randomUUID().toString();
        String randomContent = "{foo}";
        service.submit(randomID, randomContent, queryHandler);
        Assert.assertTrue(service.hasQuery(randomID));
        Assert.assertEquals(1, service.queryCount());
        Assert.assertEquals(1, service.getRunningQueries().size());
        Assert.assertEquals(queryHandler, service.getRunningQueries().get(randomID));
        Assert.assertSame(service.getQuery(randomID), queryHandler);
        Assert.assertSame(service.removeQuery(randomID), queryHandler);
        Assert.assertFalse(service.hasQuery(randomID));
        Assert.assertEquals(0, service.queryCount());
        Assert.assertEquals(0, service.getRunningQueries().size());
        verify(publisher).send(randomID, randomContent);
        service.close();
    }

    @Test
    public void testSubmitFailsFromPublisherError() throws Exception {
        Publisher publisher = Mockito.mock(Publisher.class);
        Subscriber subscriber = new MockSubscriber();
        QueryHandler queryHandler = Mockito.mock(QueryHandler.class);
        doThrow(new PubSubException("")).when(publisher).send(any(), any());
        QueryService service = new QueryService(singletonList(publisher), singletonList(subscriber), 1);
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
        QueryService service = new QueryService(singletonList(publisher), singletonList(subscriber), 1);
        service.getRunningQueries().put("id", queryHandler);

        QueryHandler actualHandler = service.killQuery("id");
        Assert.assertSame(actualHandler, queryHandler);
        Assert.assertEquals(0, service.queryCount());
        Assert.assertEquals(0, service.getRunningQueries().size());
        verify(publisher).send(new PubSubMessage("id", null, new Metadata(Metadata.Signal.KILL, null)));
        service.close();
    }

    @Test
    public void testKillQueryWhenPublisherFails() throws Exception {
        Publisher publisher = Mockito.mock(Publisher.class);
        Subscriber subscriber = new MockSubscriber();
        QueryHandler queryHandler = Mockito.mock(QueryHandler.class);
        QueryService service = new QueryService(singletonList(publisher), singletonList(subscriber), 1);
        service.getRunningQueries().put("id", queryHandler);

        doThrow(new PubSubException("")).when(publisher).send(any());
        service.killQuery("id");
        Assert.assertEquals(0, service.getRunningQueries().size());
        service.close();
    }

    @Test
    public void testFailQuery() throws Exception {
        Publisher publisher = Mockito.mock(Publisher.class);
        Subscriber subscriber = new MockSubscriber();
        QueryHandler queryHandler = Mockito.mock(QueryHandler.class);
        QueryService service = new QueryService(singletonList(publisher), singletonList(subscriber), 1);

        Assert.assertFalse(service.failQuery("id"));
        verifyZeroInteractions(queryHandler);
        service.getRunningQueries().put("id", queryHandler);
        Assert.assertTrue(service.failQuery("id"));
        verify(queryHandler).fail();
    }

    @Test
    public void testQueryCount() throws Exception {
        Publisher publisher = Mockito.mock(Publisher.class);
        Subscriber subscriber = new MockSubscriber();
        QueryHandler queryHandler = Mockito.mock(QueryHandler.class);
        QueryService service = new QueryService(singletonList(publisher), singletonList(subscriber), 1);

        Assert.assertEquals(service.queryCount(), 0);
        service.getRunningQueries().put("id", queryHandler);
        Assert.assertEquals(service.queryCount(), 1);
    }

    @Test
    public void testSendingSignalForQuery() throws Exception {
        Publisher publisher = Mockito.mock(Publisher.class);
        Subscriber subscriber = new MockSubscriber();
        QueryService service = new QueryService(singletonList(publisher), singletonList(subscriber), 1);

        service.sendSignal("id", Metadata.Signal.COMPLETE);
        verify(publisher).send(new PubSubMessage("id", null, new Metadata(Metadata.Signal.COMPLETE, null)));
    }

    @Test
    public void testSendingMessage() throws Exception {
        Publisher publisher = Mockito.mock(Publisher.class);
        Subscriber subscriber = new MockSubscriber();
        QueryService service = new QueryService(singletonList(publisher), singletonList(subscriber), 1);

        service.sendMessage(new PubSubMessage("foo", null, new Metadata(Metadata.Signal.ACKNOWLEDGE, "test")));
        verify(publisher).send(new PubSubMessage("foo", null, new Metadata(Metadata.Signal.ACKNOWLEDGE, "test")));
    }
}
