/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.rest.model.BQLError;
import com.yahoo.bullet.rest.common.BQLException;
import com.yahoo.bullet.rest.common.Utils;
import com.yahoo.bullet.rest.model.WebSocketRequest;
import com.yahoo.bullet.rest.query.QueryError;
import com.yahoo.bullet.rest.query.WebSocketQueryHandler;
import com.yahoo.bullet.rest.service.PreprocessingService;
import com.yahoo.bullet.rest.service.StatusService;
import com.yahoo.bullet.rest.service.WebSocketService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.stereotype.Controller;

@Controller
public class WebSocketController {
    private WebSocketService webSocketService;
    private PreprocessingService preprocessingService;
    private StatusService statusService;

    /**
     * Constructor that takes various services.
     *
     * @param webSocketService The {@link WebSocketService} to use.
     * @param preprocessingService The {@link PreprocessingService} to use.
     * @param statusService The {@link StatusService} to use.
     */
    @Autowired
    public WebSocketController(WebSocketService webSocketService, PreprocessingService preprocessingService,
                               StatusService statusService) {
        this.webSocketService = webSocketService;
        this.preprocessingService = preprocessingService;
        this.statusService = statusService;
    }

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
        String queryID = Utils.getNewQueryID();
        String sessionID = headerAccessor.getSessionId();
        WebSocketQueryHandler queryHandler = new WebSocketQueryHandler(webSocketService, sessionID, queryID);
        if (!statusService.isBackendStatusOk()) {
            queryHandler.fail(QueryError.SERVICE_UNAVAILABLE);
            return;
        }
        try {
            String query = preprocessingService.convertIfBQL(request.getContent());
            if (preprocessingService.queryLimitReached()) {
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
        webSocketService.killQuery(headerAccessor.getSessionId(), request.getContent());
    }
}
