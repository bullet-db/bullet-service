/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.rest.model.WebSocketRequest;
import com.yahoo.bullet.rest.service.QueryService;
import com.yahoo.bullet.rest.service.WebSocketService;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
public class WebSocketControllerTest extends AbstractTestNGSpringContextTests {
    @Autowired
    @InjectMocks
    private WebSocketController webSocketController;
    @Mock
    private WebSocketService webSocketService;
    @Mock
    private QueryService queryService;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testSubmitNewQuery() {
        doReturn(0).when(queryService).runningQueryCount();
        String sessionID = "sessionID";
        String query = "{}";
        WebSocketRequest request = new WebSocketRequest();
        request.setType(WebSocketRequest.Type.NEW_QUERY);
        request.setContent(query);
        SimpMessageHeaderAccessor headerAccessor = mock(SimpMessageHeaderAccessor.class);
        when(headerAccessor.getSessionId()).thenReturn(sessionID);

        webSocketController.submitWebsocketQuery(request, headerAccessor);

        verify(webSocketService).submitQuery(anyString(), eq(sessionID), eq(query), any());
    }

    @Test
    public void testSubmitQueryTooManyQueries() {
        doReturn(500).when(queryService).runningQueryCount();
        String sessionID = "sessionID";
        String query = "{}";
        WebSocketRequest request = new WebSocketRequest();
        request.setType(WebSocketRequest.Type.NEW_QUERY);
        request.setContent(query);
        SimpMessageHeaderAccessor headerAccessor = mock(SimpMessageHeaderAccessor.class);
        when(headerAccessor.getSessionId()).thenReturn(sessionID);

        webSocketController.submitWebsocketQuery(request, headerAccessor);

        verify(webSocketService, never()).submitQuery(any(), any(), any(), any());
    }

    @Test
    public void testSubmitBadQuery() {
        String sessionID = "sessionID";
        String query = "This is a bad query";
        WebSocketRequest request = new WebSocketRequest();
        request.setType(WebSocketRequest.Type.NEW_QUERY);
        request.setContent(query);
        SimpMessageHeaderAccessor headerAccessor = mock(SimpMessageHeaderAccessor.class);
        when(headerAccessor.getSessionId()).thenReturn(sessionID);

        webSocketController.submitWebsocketQuery(request, headerAccessor);

        verify(webSocketService, never()).submitQuery(any(), any(), any(), any());
    }

    @Test
    public void testSubmitBadQueryRuntimeException() {
        String sessionID = "sessionID";
        WebSocketRequest request = mock(WebSocketRequest.class);
        doThrow(RuntimeException.class).when(request).getContent();
        doReturn(WebSocketRequest.Type.NEW_QUERY).when(request).getType();

        SimpMessageHeaderAccessor headerAccessor = mock(SimpMessageHeaderAccessor.class);
        when(headerAccessor.getSessionId()).thenReturn(sessionID);

        webSocketController.submitWebsocketQuery(request, headerAccessor);

        verify(webSocketService, never()).submitQuery(any(), any(), any(), any());
    }

    @Test
    public void testSubmitKillQuery() {
        WebSocketRequest request = new WebSocketRequest();
        request.setType(WebSocketRequest.Type.KILL_QUERY);
        request.setContent("queryID");
        SimpMessageHeaderAccessor headerAccessor = mock(SimpMessageHeaderAccessor.class);
        when(headerAccessor.getSessionId()).thenReturn("sessionID");

        webSocketController.submitWebsocketQuery(request, headerAccessor);

        verify(webSocketService).sendKillSignal(eq("sessionID"), eq("queryID"));
    }
}
