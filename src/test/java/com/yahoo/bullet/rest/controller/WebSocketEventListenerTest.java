/*
 *  Copyright 2018 Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.rest.service.WebSocketService;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class WebSocketEventListenerTest {
    @Test
    public void testSubmitNewQuery() {
        WebSocketService webSocketService = mock(WebSocketService.class);
        WebSocketEventListener webSocketEventListener = new WebSocketEventListener(webSocketService);
        SessionDisconnectEvent event = mock(SessionDisconnectEvent.class);

        Message<byte[]> message = mock(Message.class);
        doReturn(message).when(event).getMessage();
        Map<String, Object> headers = new HashMap<>();
        headers.put(SimpMessageHeaderAccessor.SESSION_ID_HEADER, "foo");
        doReturn(new MessageHeaders(headers)).when(message).getHeaders();

        webSocketEventListener.handleWebSocketDisconnectListener(event);

        verify(webSocketService).killQuery(eq("foo"), eq(null));
    }
}
