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
        Mockito.verify(queryHandler).fail();
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
        Mockito.verify(queryHandler).fail(any(QueryError.class));
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
        Assert.assertEquals(1, service.getRunningQueries().size());
        Assert.assertEquals(queryHandler, service.getRunningQueries().get(randomID));
        Mockito.verify(publisher).send(randomID, randomContent);
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
        Mockito.verify(queryHandler).fail(QueryError.SERVICE_UNAVAILABLE);
    }

    @Test
    public void testSubmitSignal() throws Exception {
        Publisher publisher = Mockito.mock(Publisher.class);
        Subscriber subscriber = new MockSubscriber();
        QueryHandler queryHandler = Mockito.mock(QueryHandler.class);
        QueryService service = new QueryService(singletonList(publisher), singletonList(subscriber), 1);
        service.getRunningQueries().put("id", queryHandler);

        service.submitSignal("id", Metadata.Signal.KILL);
        Assert.assertEquals(0, service.getRunningQueries().size());
        Mockito.verify(publisher).send(new PubSubMessage("id", null, new Metadata(Metadata.Signal.KILL, null)));
        service.close();
    }

    @Test
    public void testSubmitSignalWhenPublisherFails() throws Exception {
        Publisher publisher = Mockito.mock(Publisher.class);
        Subscriber subscriber = new MockSubscriber();
        QueryHandler queryHandler = Mockito.mock(QueryHandler.class);
        QueryService service = new QueryService(singletonList(publisher), singletonList(subscriber), 1);
        service.getRunningQueries().put("id", queryHandler);

        doThrow(new PubSubException("")).when(publisher).send(any());
        service.submitSignal("id", Metadata.Signal.KILL);
        Assert.assertEquals(0, service.getRunningQueries().size());
        service.close();
    }

    @Test
    public void testRunningQueryCount() throws Exception {
        Publisher publisher = Mockito.mock(Publisher.class);
        Subscriber subscriber = new MockSubscriber();
        QueryHandler queryHandler = Mockito.mock(QueryHandler.class);
        QueryService service = new QueryService(singletonList(publisher), singletonList(subscriber), 1);

        Assert.assertEquals(service.runningQueryCount(), 0);
        service.getRunningQueries().put("id", queryHandler);
        Assert.assertEquals(service.runningQueryCount(), 1);
    }
}
