/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.rest.model.WebSocketRequest;
import com.yahoo.bullet.rest.query.BQLError;
import com.yahoo.bullet.rest.query.BQLException;
import com.yahoo.bullet.rest.query.QueryError;
import com.yahoo.bullet.rest.query.WebSocketQueryHandler;
import com.yahoo.bullet.rest.service.BackendStatusService;
import com.yahoo.bullet.rest.service.PreprocessingService;
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
    @Autowired
    private PreprocessingService preprocessingService;
    @Autowired
    private QueryService queryService;
    @Autowired
    private BackendStatusService backendStatusService;

    /**
     * The method that handles WebSocket messages to this endpoint.
     *
     * @param request The {@link WebSocketRequest} object.
     * @param headerAccessor The {@link SimpMessageHeaderAccessor} headers associated with the message.
     */
    @MessageMapping("${bullet.websocket.server.destination}")
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
        WebSocketQueryHandler queryHandler = new WebSocketQueryHandler(webSocketService, sessionID, queryID);
        if (!backendStatusService.isBackendStatusOk()) {
            queryHandler.fail(QueryError.SERVICE_UNAVAILABLE);
            return;
        }
        try {
            String query = preprocessingService.convertIfBQL(request.getContent());
            if (preprocessingService.queryLimitReached(queryService)) {
                queryHandler.fail(QueryError.TOO_MANY_QUERIES);
            } else {
                webSocketService.submitQuery(queryID, sessionID, query, queryHandler);
            }
        } catch (BQLException e) {
            queryHandler.fail(new BQLError(e));
        } catch (Exception e) {
            queryHandler.fail(QueryError.INVALID_QUERY);
        }
    }

    private void handleKillQuery(WebSocketRequest request, SimpMessageHeaderAccessor headerAccessor) {
        webSocketService.sendKillSignal(headerAccessor.getSessionId(), request.getContent());
    }
}
