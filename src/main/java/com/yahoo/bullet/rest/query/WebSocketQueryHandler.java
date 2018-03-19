/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.query;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.rest.model.WebSocketResponse;
import com.yahoo.bullet.rest.service.WebSocketService;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessageType;

import java.util.HashMap;
import java.util.Map;

/**
 * Query handler that implements results for WebSocket - multiple results per query.
 */
public class WebSocketQueryHandler extends QueryHandler {
    private WebSocketService webSocketService;
    private String sessionID;
    private String queryID;
    private SimpMessageHeaderAccessor headerAccessor;

    private static final Map<Metadata.Signal, WebSocketResponse.Type> MESSAGE_TYPE_MAP = new HashMap<>();
    static {
        MESSAGE_TYPE_MAP.put(Metadata.Signal.FAIL, WebSocketResponse.Type.FAIL);
        MESSAGE_TYPE_MAP.put(Metadata.Signal.COMPLETE, WebSocketResponse.Type.COMPLETE);
    }

    /**
     * Constructor method.
     *
     * @param webSocketService The {@link WebSocketService} to handle websocket messages.
     * @param sessionID The session ID to represent the client.
     * @param queryID The query ID.
     */
    public WebSocketQueryHandler(WebSocketService webSocketService, String sessionID, String queryID) {
        this.webSocketService = webSocketService;
        this.sessionID = sessionID;
        this.queryID = queryID;
        headerAccessor = SimpMessageHeaderAccessor.create(SimpMessageType.MESSAGE);
        headerAccessor.setSessionId(sessionID);
    }

    @Override
    public void complete() {
        super.complete();
        webSocketService.deleteSession(sessionID);
    }

    @Override
    public void send(PubSubMessage response) {
        if (!isComplete()) {
            WebSocketResponse responseMessage = new WebSocketResponse(getType(response), response.getContent());
            webSocketService.sendResponse(sessionID, responseMessage, headerAccessor);
        }
    }

    @Override
    public void fail(QueryError cause) {
        if (!isComplete()) {
            WebSocketResponse responseMessage = new WebSocketResponse(WebSocketResponse.Type.FAIL, cause.toString());
            webSocketService.sendResponse(sessionID, responseMessage, headerAccessor);
            complete();
        }
    }

    @Override
    public void acknowledge() {
        WebSocketResponse response = new WebSocketResponse(WebSocketResponse.Type.ACK, queryID);
        webSocketService.sendResponse(sessionID, response, headerAccessor);
    }

    private WebSocketResponse.Type getType(PubSubMessage message) {
        if (message.hasSignal()) {
            return MESSAGE_TYPE_MAP.getOrDefault(message.getMetadata().getSignal(), WebSocketResponse.Type.MESSAGE);
        }
        return WebSocketResponse.Type.MESSAGE;
    }
}

