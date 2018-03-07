/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.rest.query.HTTPQueryHandler;
import com.yahoo.bullet.rest.query.QueryError;
import com.yahoo.bullet.rest.query.SseQueryHandler;
import com.yahoo.bullet.rest.service.QueryService;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
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

    @PostMapping("/sse")
    public SseEmitter streamingQuery(@RequestBody String query) {
        SseEmitter sseEmitter = new SseEmitter();
        String queryID = UUID.randomUUID().toString();
        SseQueryHandler sseQueryHandler = new SseQueryHandler(queryID, sseEmitter, queryService);
        if (query == null) {
            sseQueryHandler.fail(QueryError.INVALID_QUERY);
        } else {
            queryService.submit(queryID, query, sseQueryHandler);
        }
        return sseEmitter;
    }
}
