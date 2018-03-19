/*
 *  Copyright 2018 Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.rest.service.WebSocketService;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
public class WebSocketEventListenerTest extends AbstractTestNGSpringContextTests {
    @Autowired
    @InjectMocks
    private WebSocketEventListener webSocketEventListener;
    @Mock
    private WebSocketService webSocketService;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testSubmitNewQuery() {
        SessionDisconnectEvent event = mock(SessionDisconnectEvent.class);
        Message<byte[]> message = mock(Message.class);
        when(event.getMessage()).thenReturn(message);
        Map<String, Object> headers = new HashMap<>();
        headers.put(SimpMessageHeaderAccessor.SESSION_ID_HEADER, "foo");
        when(message.getHeaders()).thenReturn(new MessageHeaders(headers));

        webSocketEventListener.handleWebSocketDisconnectListener(event);

        verify(webSocketService).sendKillSignal(eq("foo"), eq(null));
    }
}
