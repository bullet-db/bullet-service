/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.rest.model.WebSocketRequest;
import com.yahoo.bullet.rest.query.HTTPQueryHandler;
import com.yahoo.bullet.rest.query.QueryError;
import com.yahoo.bullet.rest.query.SSEQueryHandler;
import com.yahoo.bullet.rest.service.QueryService;
import com.yahoo.bullet.rest.service.WebSocketService;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RestController
public class QueryController {
    @Autowired @Setter
    private QueryService queryService;
    @Autowired
    private WebSocketService webSocketService;

    /**
     * The method that handles POSTs to this endpoint. Consumes the HTTP request, invokes {@link QueryService} to
     * register and transmit the query to Bullet.
     *
     * @param query The JSON query.
     * @return A {@link CompletableFuture} representing the eventual result.
     */
    @PostMapping(path = "/query", consumes = { MediaType.TEXT_PLAIN_VALUE }, produces = { MediaType.APPLICATION_JSON_VALUE })
    public CompletableFuture<String> submitQuery(@RequestBody String query) {
        HTTPQueryHandler queryHandler = new HTTPQueryHandler();
        if (query == null) {
            queryHandler.fail(QueryError.INVALID_QUERY);
        } else {
            String queryID = UUID.randomUUID().toString();
            queryService.submit(queryID, query, queryHandler);
        }
        return queryHandler.getResult();
    }

    /**
     * The method that handles SSE POSTs to this endpoint. Consumes the HTTP request, invokes {@link QueryService} to
     * register and transmit the query to Bullet.
     *
     * @param query The JSON query.
     * @return A {@link SseEmitter} to send streaming results.
     */
    @PostMapping("/querySSE")
    public SseEmitter submitSSEQuery(@RequestBody String query) {
        SseEmitter sseEmitter = new SseEmitter();
        String queryID = UUID.randomUUID().toString();
        SSEQueryHandler sseQueryHandler = new SSEQueryHandler(queryID, sseEmitter, queryService);
        // query should not be null at this point. If the post body is null, Springframework will return 400 directly.
        queryService.submit(queryID, query, sseQueryHandler);
        return sseEmitter;
    }

    /**
     * The method that handles WebSocket messages to this endpoint.
     *
     * @param request The {@link WebSocketRequest} object.
     * @param headerAccessor The {@link SimpMessageHeaderAccessor} headers associated with the message.
     */
    @MessageMapping("/queryWS")
    public void submitWebsocketQuery(@Payload WebSocketRequest request, SimpMessageHeaderAccessor headerAccessor) {
        WebSocketRequest.RequestType type = request.getType();
        switch (type) {
            case NEW_QUERY:
                String queryID = UUID.randomUUID().toString();
                webSocketService.submitQuery(queryID, request.getContent(), headerAccessor);
                break;
            case KILL_QUERY:
                webSocketService.sendKillSignal(headerAccessor.getSessionId(), request.getContent());
                break;
        }
    }
}
