/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.rest.model.WebSocketRequest;
import com.yahoo.bullet.rest.service.WebSocketService;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessageType;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
public class WebSocketControllerTest extends AbstractTestNGSpringContextTests {
    @Autowired @InjectMocks
    private WebSocketController webSocketController;
    @Mock
    private WebSocketService webSocketService;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testSubmitNewQuery() {
        WebSocketRequest request = new WebSocketRequest();
        request.setType(WebSocketRequest.RequestType.NEW_QUERY);
        request.setContent("foo");
        SimpMessageHeaderAccessor headerAccessor = SimpMessageHeaderAccessor.create(SimpMessageType.MESSAGE);

        webSocketController.submitRequest(request, headerAccessor);

        verify(webSocketService).submitQuery(eq("foo"), eq(headerAccessor));
    }

    @Test
    public void testSubmitKillQuery() {
        WebSocketRequest request = new WebSocketRequest();
        request.setType(WebSocketRequest.RequestType.KILL_QUERY);
        request.setContent("queryID");
        SimpMessageHeaderAccessor headerAccessor = mock(SimpMessageHeaderAccessor.class);
        when(headerAccessor.getSessionId()).thenReturn("sessionID");

        webSocketController.submitRequest(request, headerAccessor);

        verify(webSocketService).sendKillSignal(eq("sessionID"), eq("queryID"));
    }
}
