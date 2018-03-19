/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.query;

import com.yahoo.bullet.pubsub.PubSubMessage;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;

public class HTTPQueryHandlerTest {
    @Test
    public void testCompleteOnSendingOneMessage() throws Exception {
        HTTPQueryHandler queryHandler = new HTTPQueryHandler();
        CompletableFuture<String> result = queryHandler.getResult();
        Assert.assertFalse(result.isDone());

        queryHandler.send(new PubSubMessage("", "foo"));

        Assert.assertTrue(result.isDone());
        Assert.assertFalse(result.isCancelled());
        Assert.assertEquals(result.get(), "foo");
    }

    @Test
    public void testSendAfterComplete() throws Exception {
        HTTPQueryHandler queryHandler = new HTTPQueryHandler();
        CompletableFuture<String> result = queryHandler.getResult();
        Assert.assertFalse(result.isDone());

        queryHandler.send(new PubSubMessage("", "foo"));
        queryHandler.complete();
        queryHandler.send(new PubSubMessage("", "bar"));

        Assert.assertTrue(result.isDone());
        Assert.assertFalse(result.isCancelled());
        Assert.assertEquals(result.get(), "foo");
    }

    @Test
    public void testCompleteOnSendingOneFail() throws Exception {
        HTTPQueryHandler queryHandler = new HTTPQueryHandler();
        CompletableFuture<String> result = queryHandler.getResult();
        Assert.assertFalse(result.isDone());

        QueryError cause = new QueryError("foo", "bar");
        queryHandler.fail(cause);

        Assert.assertTrue(result.isDone());
        Assert.assertFalse(result.isCancelled());
        Assert.assertEquals(result.get(), cause.toString());
    }

    @Test
    public void testFailAfterComplete() throws Exception {
        HTTPQueryHandler queryHandler = new HTTPQueryHandler();
        CompletableFuture<String> result = queryHandler.getResult();
        Assert.assertFalse(result.isDone());

        queryHandler.fail(QueryError.SERVICE_UNAVAILABLE);
        queryHandler.complete();
        queryHandler.fail(new QueryError("foo", "bar"));

        Assert.assertTrue(result.isDone());
        Assert.assertEquals(result.get(), QueryError.SERVICE_UNAVAILABLE.toString());
    }

    @Test
    public void testAcknowledgeDoesNothing() {
        HTTPQueryHandler queryHandler = new HTTPQueryHandler();
        queryHandler.acknowledge();
        CompletableFuture<String> result = queryHandler.getResult();
        Assert.assertFalse(result.isDone());
    }
}
