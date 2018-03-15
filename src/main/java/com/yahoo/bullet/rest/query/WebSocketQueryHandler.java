/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.query;

import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.rest.model.WebSocketResponse;
import com.yahoo.bullet.rest.service.WebSocketService;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessageType;

/**
 * Query handler that implements results for WebSocket - multiple results per query.
 */
public class WebSocketQueryHandler extends QueryHandler {
    private WebSocketService webSocketService;
    private String sessionID;
    private String queryID;
    private SimpMessageHeaderAccessor headerAccessor;

    /**
     * Constructor method.
     *
     * @param webSocketService The {@link WebSocketService} to handle websocket messages.
     * @param sessionID TThe session ID to represent the client.
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
        webSocketService.removeSessionID(sessionID);
    }

    @Override
    public void send(PubSubMessage response) {
        if (!isComplete()) {
            WebSocketResponse responseMessage = new WebSocketResponse(WebSocketResponse.Type.CONTENT, response.getContent());
            webSocketService.sendResponse(sessionID, responseMessage, headerAccessor);
        }
    }

    @Override
    public void fail(QueryError cause) {
        if (!isComplete()) {
            WebSocketResponse responseMessage =
                    new WebSocketResponse(WebSocketResponse.Type.FAIL, cause.toString());
            webSocketService.sendResponse(sessionID, responseMessage, headerAccessor);
            complete();
        }
    }

    @Override
    public void acknowledge() {
        WebSocketResponse response = new WebSocketResponse(WebSocketResponse.Type.ACK, queryID);
        webSocketService.sendResponse(sessionID, response, headerAccessor);
    }
}

