/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.rest.query.BQLError;
import com.yahoo.bullet.rest.query.BQLException;
import com.yahoo.bullet.rest.query.HTTPQueryHandler;
import com.yahoo.bullet.rest.query.QueryError;
import com.yahoo.bullet.rest.query.SSEQueryHandler;
import com.yahoo.bullet.rest.service.BackendStatusService;
import com.yahoo.bullet.rest.service.PreprocessingService;
import com.yahoo.bullet.rest.service.QueryService;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.concurrent.CompletableFuture;

@RestController
public class HTTPQueryController {
    @Autowired @Setter
    private QueryService queryService;

    @Autowired
    private PreprocessingService preprocessingService;

    @Autowired
    private BackendStatusService backendStatusService;

    /**
     * The method that handles POSTs to this endpoint. Consumes the HTTP request, invokes {@link QueryService} to
     * register and transmit the query to Bullet.
     *
     * @param query The JSON query.
     * @return A {@link CompletableFuture} representing the eventual result.
     */
    @PostMapping(path = "${bullet.endpoint.http}", consumes = { MediaType.TEXT_PLAIN_VALUE }, produces = { MediaType.APPLICATION_JSON_VALUE })
    @SuppressWarnings("unchecked")
    public CompletableFuture<String> submitHTTPQuery(@RequestBody String query) {
        HTTPQueryHandler queryHandler = new HTTPQueryHandler();
        String queryID = QueryService.getNewQueryID();
        if (!backendStatusService.isBackendStatusOk()) {
            queryHandler.fail(QueryError.SERVICE_UNAVAILABLE);
            return queryHandler.getResult();
        }
        try {
            query = preprocessingService.convertIfBQL(query);
            if (preprocessingService.containsWindow(query)) {
                queryHandler.fail(QueryError.UNSUPPORTED_QUERY);
            } else if (preprocessingService.queryLimitReached(queryService)) {
                queryHandler.fail(QueryError.TOO_MANY_QUERIES);
            } else {
                queryService.submit(queryID, query, queryHandler);
            }
        } catch (BQLException e) {
            queryHandler.fail(new BQLError(e));
        } catch (Exception e) {
            queryHandler.fail(QueryError.INVALID_QUERY);
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
    @PostMapping(value = "${bullet.endpoint.sse}", consumes = { MediaType.TEXT_PLAIN_VALUE }, produces = { MediaType.APPLICATION_JSON_VALUE })
    public SseEmitter submitSSEQuery(@RequestBody String query) {
        SseEmitter sseEmitter = new SseEmitter();
        String queryID = QueryService.getNewQueryID();
        SSEQueryHandler sseQueryHandler = new SSEQueryHandler(queryID, sseEmitter, queryService);
        if (!backendStatusService.isBackendStatusOk()) {
            sseQueryHandler.fail(QueryError.SERVICE_UNAVAILABLE);
            return sseEmitter;
        }
        try {
            query = preprocessingService.convertIfBQL(query);
            if (preprocessingService.queryLimitReached(queryService)) {
                sseQueryHandler.fail(QueryError.TOO_MANY_QUERIES);
            } else {
                queryService.submit(queryID, query, sseQueryHandler);
            }
        } catch (BQLException e) {
            sseQueryHandler.fail(new BQLError(e));
        } catch (Exception e) {
            sseQueryHandler.fail(QueryError.INVALID_QUERY);
        }
        return sseEmitter;
    }
}
