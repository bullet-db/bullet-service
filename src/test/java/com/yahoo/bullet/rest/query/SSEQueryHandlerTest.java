/*
 *  Copyright 2018 Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.query;

import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.rest.service.QueryService;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class SSEQueryHandlerTest {
    private QueryService queryService;
    private SseEmitter sseEmitter;

    @BeforeMethod
    public void setup() {
        queryService = mock(QueryService.class);
        sseEmitter = mock(SseEmitter.class);
    }

    @Test
    public void testSendOnMessage() throws Exception {
        PubSubMessage message = new PubSubMessage("id", "foo");

        SSEQueryHandler sseQueryHandler = new SSEQueryHandler("id", sseEmitter, queryService);
        sseQueryHandler.send(message);

        verify(sseEmitter).send(eq(message.getContent()), eq(MediaType.APPLICATION_JSON));
        Assert.assertFalse(sseQueryHandler.isComplete());
    }

    @Test
    public void testSendOnException() throws Exception {
        PubSubMessage message = new PubSubMessage("id", "foo");

        doThrow(new IOException()).when(sseEmitter).send(message.getContent(), MediaType.APPLICATION_JSON);
        SSEQueryHandler sseQueryHandler = new SSEQueryHandler("id", sseEmitter, queryService);
        sseQueryHandler.send(message);

        verify(queryService).kill("id");
        Assert.assertTrue(sseQueryHandler.isComplete());
    }

    @Test
    public void testSendAfterComplete() throws Exception {
        SSEQueryHandler sseQueryHandler = new SSEQueryHandler("id", sseEmitter, queryService);
        sseQueryHandler.complete();
        sseQueryHandler.send(new PubSubMessage("id", "foo"));

        verify(sseEmitter, never()).send(any(), any());
        verify(queryService, never()).kill(any());
        Assert.assertTrue(sseQueryHandler.isComplete());
    }

    @Test
    public void testFailOnCause() throws Exception {
        SSEQueryHandler sseQueryHandler = new SSEQueryHandler("id", sseEmitter, queryService);
        sseQueryHandler.fail(QueryError.SERVICE_UNAVAILABLE);

        verify(sseEmitter).send(eq(QueryError.SERVICE_UNAVAILABLE.toString()), eq(MediaType.APPLICATION_JSON));
        Assert.assertTrue(sseQueryHandler.isComplete());
    }

    @Test
    public void testFailOnException() throws Exception {
        doThrow(new IOException()).when(sseEmitter).send(QueryError.SERVICE_UNAVAILABLE.toString(), MediaType.APPLICATION_JSON);
        SSEQueryHandler sseQueryHandler = new SSEQueryHandler("id", sseEmitter, queryService);
        sseQueryHandler.fail(QueryError.SERVICE_UNAVAILABLE);

        verify(queryService).kill("id");
        Assert.assertTrue(sseQueryHandler.isComplete());
    }

    @Test
    public void testFailAfterComplete() throws Exception {
        SSEQueryHandler sseQueryHandler = new SSEQueryHandler("id", sseEmitter, queryService);
        sseQueryHandler.complete();
        sseQueryHandler.fail(QueryError.SERVICE_UNAVAILABLE);

        verify(sseEmitter, never()).send(any(), any());
        verify(queryService, never()).kill(any());
        Assert.assertTrue(sseQueryHandler.isComplete());
    }
}
