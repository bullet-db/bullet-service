/*
 *  Copyright 2018 Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.rest.model.WebSocketResponse;
import com.yahoo.bullet.rest.query.QueryHandler;
import com.yahoo.bullet.rest.query.WebSocketQueryHandler;
import org.mockito.ArgumentCaptor;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.yahoo.bullet.rest.TestHelpers.assertEqualsBql;
import static com.yahoo.bullet.rest.TestHelpers.assertEqualsQuery;
import static com.yahoo.bullet.rest.TestHelpers.getBQLQuery;
import static com.yahoo.bullet.rest.TestHelpers.getQuery;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WebSocketServiceTest {
    private WebSocketService webSocketService;
    private SimpMessagingTemplate simpMessagingTemplate;
    private HandlerService handlerService;
    private QueryService queryService;

    @BeforeMethod
    public void setup() {
        queryService = mock(QueryService.class);
        handlerService = mock(HandlerService.class);
        simpMessagingTemplate = mock(SimpMessagingTemplate.class);
        webSocketService = new WebSocketService(queryService, handlerService, simpMessagingTemplate, "/response");
    }

    @Test
    public void testSendKillSignalWithNonExistingSessionID() {
        webSocketService.getSessionIDMap().clear();
        webSocketService.killQuery("sessionID", null);

        verify(handlerService, never()).removeHandler(any());
        verify(queryService, never()).kill(any());
    }

    @Test
    public void testSendKillSignalWithMissingQueryID() {
        webSocketService.getSessionIDMap().put("sessionID", "queryID");
        webSocketService.killQuery("sessionID", null);

        verify(handlerService).removeHandler(any());
        verify(queryService).kill(eq("queryID"));
    }

    @Test
    public void testSendNoKillSignalIfDifferentQueryIDThanInSession() {
        webSocketService.getSessionIDMap().put("sessionID", "queryID");
        webSocketService.killQuery("sessionID", "differentQueryID");

        verify(handlerService, never()).removeHandler(any());
        verify(queryService, never()).kill(any());
    }

    @Test
    public void testSendKillSignalWithExistingSessionID() {
        webSocketService.getSessionIDMap().put("sessionID", "queryID");
        webSocketService.killQuery("sessionID", "queryID");

        verify(handlerService).removeHandler("queryID");
        verify(queryService).kill("queryID");
        Assert.assertFalse(webSocketService.getSessionIDMap().containsKey("sessionID"));
    }

    @Test
    public void testSubmitQuery() {
        String sessionID = "sessionID";
        String queryID = "queryID";
        WebSocketQueryHandler handler = new WebSocketQueryHandler(webSocketService, sessionID, queryID);
        webSocketService.submitQuery(queryID, sessionID, getQuery(), getBQLQuery(), handler);

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<String> bqlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<QueryHandler> handlerCaptor = ArgumentCaptor.forClass(QueryHandler.class);
        verify(queryService).submit(eq(queryID), queryCaptor.capture(), bqlCaptor.capture());
        verify(handlerService).addHandler(eq(queryID), handlerCaptor.capture());

        assertEqualsQuery(queryCaptor.getValue());
        assertEqualsBql(bqlCaptor.getValue());
        Assert.assertSame(handlerCaptor.getValue(), handler);
        Assert.assertTrue(webSocketService.getSessionIDMap().containsKey(sessionID));
    }

    @Test
    public void testSendResponse() {
        String sessionID = "sessionID";
        SimpMessageHeaderAccessor headerAccessor = mock(SimpMessageHeaderAccessor.class);
        when(headerAccessor.getSessionId()).thenReturn(sessionID);
        when(headerAccessor.getMessageHeaders()).thenReturn(null);

        WebSocketResponse response = new WebSocketResponse(WebSocketResponse.Type.ACK, "foo");
        webSocketService.sendResponse(sessionID, response, headerAccessor);

        verify(simpMessagingTemplate).convertAndSendToUser("sessionID", "/response", response, (MessageHeaders) null);
    }
}
