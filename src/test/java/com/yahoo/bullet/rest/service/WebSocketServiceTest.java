/*
 *  Copyright 2018 Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.rest.model.WebSocketResponse;
import com.yahoo.bullet.rest.query.WebSocketQueryHandler;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
public class WebSocketServiceTest extends AbstractTestNGSpringContextTests {
    @Autowired @InjectMocks
    private WebSocketService webSocketService;
    @Mock
    private SimpMessagingTemplate simpMessagingTemplate;
    @Mock
    private QueryService queryService;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testSendKillSignalWithNonExistingSessionID() {
        webSocketService.getSessionIDMap().clear();
        webSocketService.sendKillSignal("sessionID", null);

        verify(queryService, never()).submitSignal(any(), any());
    }

    @Test
    public void testSendKillSignalWithExistingSessionID() {
        webSocketService.getSessionIDMap().put("sessionID", "queryID");
        webSocketService.sendKillSignal("sessionID", "queryID");

        verify(queryService).submitSignal("queryID", Metadata.Signal.KILL);
        Assert.assertFalse(webSocketService.getSessionIDMap().containsKey("sessionID"));
    }

    @Test
    public void testSubmitQuery() {
        String sessionID = "sessionID";
        String queryID = "queryID";
        webSocketService.getSessionIDMap().clear();
        webSocketService.submitQuery(queryID, sessionID, "foo", new WebSocketQueryHandler(webSocketService, sessionID, queryID));

        verify(queryService).submit(eq(queryID), eq("foo"), any());
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
