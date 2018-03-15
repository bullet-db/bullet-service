/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.rest.model.WebSocketRequest;
import com.yahoo.bullet.rest.service.QueryService;
import com.yahoo.bullet.rest.service.WebSocketService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.stereotype.Controller;

@Controller
public class WebSocketController {
    @Autowired
    private WebSocketService webSocketService;

    /**
     * The method that handles WebSocket messages to this endpoint.
     *
     * @param request The {@link WebSocketRequest} object.
     * @param headerAccessor The {@link SimpMessageHeaderAccessor} headers associated with the message.
     */
    @MessageMapping("/submit.request")
    public void submitWebsocketQuery(@Payload WebSocketRequest request, SimpMessageHeaderAccessor headerAccessor) {
        switch (request.getType()) {
            case NEW_QUERY:
                handleNewQuery(request, headerAccessor);
                break;
            case KILL_QUERY:
                handleKillQuery(request, headerAccessor);
                break;
        }
    }

    private void handleNewQuery(WebSocketRequest request, SimpMessageHeaderAccessor headerAccessor) {
        String queryID = QueryService.getNewQueryID();
        String sessionID = headerAccessor.getSessionId();
        webSocketService.submitQuery(queryID, sessionID, request.getContent());
    }

    private void handleKillQuery(WebSocketRequest request, SimpMessageHeaderAccessor headerAccessor) {
        webSocketService.sendKillSignal(headerAccessor.getSessionId(), request.getContent());
    }
}
