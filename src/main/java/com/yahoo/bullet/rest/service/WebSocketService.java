/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.rest.model.WebSocketResponse;
import com.yahoo.bullet.rest.query.WebSocketQueryHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
public class WebSocketService {
    private static final String DESTINATION =  "/response/private";
    @Autowired
    private SimpMessagingTemplate simpMessagingTemplate;

    @Autowired
    private QueryService queryService;

    private ConcurrentMap<String, String> sessionIDMap;

    public WebSocketService() {
        sessionIDMap = new ConcurrentHashMap<>();
    }

    public void sendKillSignal(String sessionID) {
        if (sessionIDMap.containsKey(sessionID)) {
            String queryID = sessionIDMap.get(sessionID);
            queryService.submitSignal(queryID, Metadata.Signal.KILL);
            removeSessionID(sessionID);
        }
    }

    public void removeSessionID(String sessionID) {
        sessionIDMap.remove(sessionID);
    }

    public void submitQuery(String queryString, SimpMessageHeaderAccessor headerAccessor) {
        String queryID = UUID.randomUUID().toString();
        String sessionId = headerAccessor.getSessionId();
        sessionIDMap.put(sessionId, queryID);

        // Send ACK with queryID.
        WebSocketResponse response =
                new WebSocketResponse(WebSocketResponse.ResponseType.ACK, queryID);

        sendResponse(sessionId, response, headerAccessor);

        WebSocketQueryHandler queryHandler = new WebSocketQueryHandler(this, sessionId);
        queryService.submit(queryID, queryString, queryHandler);
    }

    public void sendResponse(String sessionID, WebSocketResponse response, SimpMessageHeaderAccessor headerAccessor) {
        simpMessagingTemplate.convertAndSendToUser(
                sessionID, DESTINATION, response, headerAccessor.getMessageHeaders());
    }
}
