/*
 *  Copyright 2018 Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.query;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.rest.model.WebSocketResponse;
import com.yahoo.bullet.rest.service.WebSocketService;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class WebSocketQueryHandlerTest extends AbstractTestNGSpringContextTests {
    @Mock
    private WebSocketService webSocketService;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testSendOnMessage() {
        PubSubMessage message = new PubSubMessage("id", "foo");

        WebSocketQueryHandler webSocketQueryHandler = new WebSocketQueryHandler(webSocketService, "id", "foo");
        webSocketQueryHandler.send(message);

        ArgumentCaptor<WebSocketResponse> argument = ArgumentCaptor.forClass(WebSocketResponse.class);
        verify(webSocketService).sendResponse(eq("id"), argument.capture(), any());
        Assert.assertEquals(argument.getValue().getType(), WebSocketResponse.Type.MESSAGE);
        Assert.assertEquals(argument.getValue().getContent(), message.getContent());
        Assert.assertFalse(webSocketQueryHandler.isComplete());
    }

    @Test
    public void testSendOnMessageWithFailSignal() {
        PubSubMessage message = new PubSubMessage("id", "foo", new Metadata(Metadata.Signal.FAIL, null));

        WebSocketQueryHandler webSocketQueryHandler = new WebSocketQueryHandler(webSocketService, "id", "foo");
        webSocketQueryHandler.send(message);

        ArgumentCaptor<WebSocketResponse> argument = ArgumentCaptor.forClass(WebSocketResponse.class);
        verify(webSocketService).sendResponse(eq("id"), argument.capture(), any());
        Assert.assertEquals(argument.getValue().getType(), WebSocketResponse.Type.FAIL);
        Assert.assertEquals(argument.getValue().getContent(), message.getContent());
        Assert.assertFalse(webSocketQueryHandler.isComplete());
    }

    @Test
    public void testSendOnMessageWithCompleteSignal() {
        PubSubMessage message = new PubSubMessage("id", "foo", new Metadata(Metadata.Signal.COMPLETE, null));

        WebSocketQueryHandler webSocketQueryHandler = new WebSocketQueryHandler(webSocketService, "id", "foo");
        webSocketQueryHandler.send(message);

        ArgumentCaptor<WebSocketResponse> argument = ArgumentCaptor.forClass(WebSocketResponse.class);
        verify(webSocketService).sendResponse(eq("id"), argument.capture(), any());
        Assert.assertEquals(argument.getValue().getType(), WebSocketResponse.Type.COMPLETE);
        Assert.assertEquals(argument.getValue().getContent(), message.getContent());
        Assert.assertFalse(webSocketQueryHandler.isComplete());
    }

    @Test
    public void testSendAfterComplete() {
        PubSubMessage message = new PubSubMessage("id", "foo");

        WebSocketQueryHandler webSocketQueryHandler = new WebSocketQueryHandler(webSocketService, "id", "foo");
        webSocketQueryHandler.complete();
        webSocketQueryHandler.send(message);

        verify(webSocketService, never()).sendResponse(any(), any(), any());
        Assert.assertTrue(webSocketQueryHandler.isComplete());
    }

    @Test
    public void testFailOnCause() {
        WebSocketQueryHandler webSocketQueryHandler = new WebSocketQueryHandler(webSocketService, "id", "foo");
        webSocketQueryHandler.fail(QueryError.SERVICE_UNAVAILABLE);

        ArgumentCaptor<WebSocketResponse> argument = ArgumentCaptor.forClass(WebSocketResponse.class);
        verify(webSocketService).sendResponse(eq("id"), argument.capture(), any());
        Assert.assertEquals(argument.getValue().getType(), WebSocketResponse.Type.FAIL);
        Assert.assertEquals(argument.getValue().getContent(), QueryError.SERVICE_UNAVAILABLE.toString());
        Assert.assertTrue(webSocketQueryHandler.isComplete());
    }


    @Test
    public void testFailAfterComplete() {
        WebSocketQueryHandler webSocketQueryHandler = new WebSocketQueryHandler(webSocketService, "id", "foo");
        webSocketQueryHandler.complete();
        webSocketQueryHandler.fail(QueryError.SERVICE_UNAVAILABLE);

        verify(webSocketService, never()).sendResponse(any(), any(), any());
        Assert.assertTrue(webSocketQueryHandler.isComplete());
    }

    @Test
    public void testAcknowledge() {
        WebSocketQueryHandler webSocketQueryHandler = new WebSocketQueryHandler(webSocketService, "id", "foo");
        webSocketQueryHandler.acknowledge();

        ArgumentCaptor<WebSocketResponse> argument = ArgumentCaptor.forClass(WebSocketResponse.class);
        verify(webSocketService).sendResponse(eq("id"), argument.capture(), any());
        Assert.assertEquals(argument.getValue().getType(), WebSocketResponse.Type.ACK);
        Assert.assertEquals(argument.getValue().getContent(), "foo");
        Assert.assertFalse(webSocketQueryHandler.isComplete());
    }
}
