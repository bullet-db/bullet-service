/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.parsing.Query;
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
    private HandlerService handlerService;
    private QueryService queryService;
    private SimpMessagingTemplate messagingTemplate;
    private String clientDestination;

    // Exposed for testing only.
    @Getter(AccessLevel.PACKAGE)
    private Map<String, String> sessionIDMap;

    /**
     * Constructor.
     *
     * @param queryService The {@link QueryService} to use.
     * @param handlerService The {@link HandlerService} to use.
     * @param messagingTemplate The {@link SimpMessagingTemplate} to use.
     * @param clientDestination The client destination to use for websockets.
     */
    @Autowired
    public WebSocketService(QueryService queryService, HandlerService handlerService,
                            SimpMessagingTemplate messagingTemplate,
                            @Value("${bullet.websocket.client.destination}") String clientDestination) {
        this.queryService = queryService;
        this.handlerService = handlerService;
        this.messagingTemplate = messagingTemplate;
        this.clientDestination = clientDestination;
        this.sessionIDMap = new ConcurrentHashMap<>();
    }
    /**
     * Kills the query and cleans up.
     *
     * @param sessionID The session ID to represent the client.
     * @param queryID The query ID of the query to be killed or null to kill the query associated with the session.
     */
    public void killQuery(String sessionID, String queryID) {
        String queryForSession = sessionIDMap.get(sessionID);
        if (queryForSession != null && (queryID == null || queryID.equals(queryForSession))) {
            handlerService.removeHandler(queryForSession);
            deleteSession(sessionID);
            queryService.kill(queryForSession);
        }
    }

    /**
     * Deletes a session from the session id map.
     *
     * @param sessionID The session ID to be deleted.
     */
    public void deleteSession(String sessionID) {
        sessionIDMap.remove(sessionID);
    }

    /**
     * Submits a query by {@link HandlerService}.
     *
     * @param queryID The query ID to register request with.
     * @param sessionID The session ID to represent the client.
     * @param query The valid {@link Query} to submit.
     * @param queryHandler The Query Handler to submit the query.
     */
    public void submitQuery(String queryID, String sessionID, Query query, WebSocketQueryHandler queryHandler) {
        sessionIDMap.put(sessionID, queryID);
        handlerService.addHandler(queryID, queryHandler);
        queryService.submit(queryID, query);
    }

    /**
     * Sends a response to the client through WebSocket connection.
     *
     * @param sessionID The session ID to represent the client.
     * @param response The {@link WebSocketResponse} response to be sent.
     * @param headerAccessor The {@link SimpMessageHeaderAccessor} headers to be associated with the response message.
     */
    public void sendResponse(String sessionID, WebSocketResponse response, SimpMessageHeaderAccessor headerAccessor) {
        messagingTemplate.convertAndSendToUser(sessionID, clientDestination, response, headerAccessor.getMessageHeaders());
    }
}
