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
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class WebSocketService {
    private static final String DESTINATION =  "/response/private";
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
     * @param sessionID The session id to represent the client.
     * @param queryID The query id of the query to be killed or null to kill the query associated with the session.
     */
    public void sendKillSignal(String sessionID, String queryID) {
        if (sessionIDMap.containsKey(sessionID)) {
            if (queryID == null || queryID.equals(sessionIDMap.get(sessionID))) {
                queryService.submitSignal(sessionIDMap.get(sessionID), Metadata.Signal.KILL);
                removeSessionID(sessionID);
            }
        }
    }

    /**
     * Remove a session id from the session id map.
     *
     * @param sessionID The session id to be removed.
     */
    public void removeSessionID(String sessionID) {
        sessionIDMap.remove(sessionID);
    }

    /**
     * Submit a query to {@link QueryService} and send a ACK to the client by websocket connection.
     *
     * @param queryString The String version of the query.
     * @param headerAccessor The {@link SimpMessageHeaderAccessor} headers.
     */
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

    /**
     * Send a response to the client by websocket connection.
     *
     * @param sessionID The session id to represent the client.
     * @param response The {@link WebSocketResponse} response to be sent.
     * @param headerAccessor The {@link SimpMessageHeaderAccessor} headers to be associated with the response message.
     */
    public void sendResponse(String sessionID, WebSocketResponse response, SimpMessageHeaderAccessor headerAccessor) {
        simpMessagingTemplate.convertAndSendToUser(
                sessionID, DESTINATION, response, headerAccessor.getMessageHeaders());
    }
}
