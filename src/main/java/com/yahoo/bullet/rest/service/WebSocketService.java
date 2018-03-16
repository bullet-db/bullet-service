/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.rest.model.WebSocketResponse;
import com.yahoo.bullet.rest.query.WebSocketQueryHandler;
import lombok.AccessLevel;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class WebSocketService {
    @Value("${bullet.websocket.client-destination}")
    private String clientDestination;
    @Autowired
    private SimpMessagingTemplate simpMessagingTemplate;
    @Autowired
    private QueryService queryService;

    // Exposed for testing only.
    @Getter(AccessLevel.PACKAGE)
    private Map<String, String> sessionIDMap = new ConcurrentHashMap<>();

    /**
     * Send a KILL signal to the backend.
     *
     * @param sessionID The session ID to represent the client.
     * @param queryID The query ID of the query to be killed or null to kill the query associated with the session.
     */
    public void sendKillSignal(String sessionID, String queryID) {
        String queryForSession = sessionIDMap.get(sessionID);
        if (queryForSession != null && (queryID == null || queryID.equals(queryForSession))) {
            queryService.submitSignal(queryForSession, Metadata.Signal.KILL);
            removeSessionID(sessionID);
        }
    }

    /**
     * Remove a session ID from the session id map.
     *
     * @param sessionID The session ID to be removed.
     */
    public void removeSessionID(String sessionID) {
        sessionIDMap.remove(sessionID);
    }

    /**
     * Submit a query by {@link QueryService}.
     *
     * @param queryID The query ID to register request with.
     * @param sessionID The session ID to represent the client.
     * @param queryString The String version of the query.
     */
    public void submitQuery(String queryID, String sessionID, String queryString) {
        sessionIDMap.put(sessionID, queryID);
        WebSocketQueryHandler queryHandler = new WebSocketQueryHandler(this, sessionID, queryID);
        queryService.submit(queryID, queryString, queryHandler);
        queryHandler.acknowledge();
    }

    /**
     * Send a response to the client through WebSocket connection.
     *
     * @param sessionID The session ID to represent the client.
     * @param response The {@link WebSocketResponse} response to be sent.
     * @param headerAccessor The {@link SimpMessageHeaderAccessor} headers to be associated with the response message.
     */
    public void sendResponse(String sessionID, WebSocketResponse response, SimpMessageHeaderAccessor headerAccessor) {
        simpMessagingTemplate.convertAndSendToUser(sessionID, clientDestination, response, headerAccessor.getMessageHeaders());
    }
}
