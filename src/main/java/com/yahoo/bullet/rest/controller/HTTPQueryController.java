/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.rest.common.BQLError;
import com.yahoo.bullet.rest.common.BQLException;
import com.yahoo.bullet.rest.common.Utils;
import com.yahoo.bullet.rest.query.HTTPQueryHandler;
import com.yahoo.bullet.rest.query.QueryError;
import com.yahoo.bullet.rest.query.SSEQueryHandler;
import com.yahoo.bullet.rest.service.StatusService;
import com.yahoo.bullet.rest.service.HandlerService;
import com.yahoo.bullet.rest.service.PreprocessingService;
import com.yahoo.bullet.rest.service.QueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.concurrent.CompletableFuture;

@RestController
public class HTTPQueryController {
    private QueryService queryService;
    private HandlerService handlerService;
    private PreprocessingService preprocessingService;
    private StatusService statusService;

    /**
     * Constructor that takes various services.
     *
     * @param handlerService The {@link HandlerService} to use.
     * @param queryService  The {@link QueryService} to use.
     * @param preprocessingService The {@link PreprocessingService} to use.
     * @param statusService The {@link StatusService} to use.
     */
    @Autowired
    public HTTPQueryController(HandlerService handlerService, QueryService queryService,
                               PreprocessingService preprocessingService, StatusService statusService) {
        this.handlerService = handlerService;
        this.queryService = queryService;
        this.preprocessingService = preprocessingService;
        this.statusService = statusService;
    }

    /**
     * The method that handles POSTs to this endpoint. Consumes the HTTP request, invokes {@link HandlerService} to
     * register and transmit the query to Bullet.
     *
     * @param query The JSON query.
     * @return A {@link CompletableFuture} representing the eventual result.
     */
    @PostMapping(path = "${bullet.endpoint.http}", consumes = { MediaType.TEXT_PLAIN_VALUE }, produces = { MediaType.APPLICATION_JSON_VALUE })
    public CompletableFuture<String> submitHTTPQuery(@RequestBody String query) {
        HTTPQueryHandler handler = new HTTPQueryHandler();
        String id = Utils.getNewQueryID();
        if (!statusService.isBackendStatusOk()) {
            handler.fail(QueryError.SERVICE_UNAVAILABLE);
            return handler.getResult();
        }
        try {
            query = preprocessingService.convertIfBQL(query);
            if (preprocessingService.containsWindow(query)) {
                handler.fail(QueryError.UNSUPPORTED_QUERY);
            } else if (preprocessingService.queryLimitReached()) {
                handler.fail(QueryError.TOO_MANY_QUERIES);
            } else {
                handlerService.addQuery(id, handler);
                queryService.submit(id, query);
            }
        } catch (BQLException e) {
            handler.fail(new BQLError(e));
        } catch (Exception e) {
            handler.fail(QueryError.INVALID_QUERY);
        }
        return handler.getResult();
    }

    /**
     * The method that handles SSE POSTs to this endpoint. Consumes the HTTP request, invokes {@link HandlerService} to
     * register and transmit the query to Bullet.
     *
     * @param query The JSON query.
     * @return A {@link SseEmitter} to send streaming results.
     */
    @PostMapping(value = "${bullet.endpoint.sse}", consumes = { MediaType.TEXT_PLAIN_VALUE }, produces = { MediaType.APPLICATION_JSON_VALUE })
    public SseEmitter submitSSEQuery(@RequestBody String query) {
        SseEmitter sseEmitter = new SseEmitter();
        String id = Utils.getNewQueryID();
        SSEQueryHandler handler = new SSEQueryHandler(id, sseEmitter, queryService);
        if (!statusService.isBackendStatusOk()) {
            handler.fail(QueryError.SERVICE_UNAVAILABLE);
            return sseEmitter;
        }
        try {
            query = preprocessingService.convertIfBQL(query);
            if (preprocessingService.queryLimitReached()) {
                handler.fail(QueryError.TOO_MANY_QUERIES);
            } else {
                handlerService.addQuery(id, handler);
                queryService.submit(id, query);
            }
        } catch (BQLException e) {
            handler.fail(new BQLError(e));
        } catch (Exception e) {
            handler.fail(QueryError.INVALID_QUERY);
        }
        return sseEmitter;
    }
}
