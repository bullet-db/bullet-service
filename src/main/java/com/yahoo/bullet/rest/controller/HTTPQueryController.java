/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.rest.common.BQLException;
import com.yahoo.bullet.rest.common.Utils;
import com.yahoo.bullet.rest.model.AsyncQuery;
import com.yahoo.bullet.rest.model.BQLError;
import com.yahoo.bullet.rest.model.QueryResponse;
import com.yahoo.bullet.rest.query.HTTPQueryHandler;
import com.yahoo.bullet.rest.query.QueryError;
import com.yahoo.bullet.rest.query.SSEQueryHandler;
import com.yahoo.bullet.rest.service.HandlerService;
import com.yahoo.bullet.rest.service.PreprocessingService;
import com.yahoo.bullet.rest.service.QueryService;
import com.yahoo.bullet.rest.service.StatusService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;

@RestController @Slf4j
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
        if (!statusService.isBackendStatusOk()) {
            handler.fail(QueryError.SERVICE_UNAVAILABLE);
            return handler.getResult();
        }
        if (preprocessingService.queryLimitReached()) {
            handler.fail(QueryError.TOO_MANY_QUERIES);
            return handler.getResult();
        }
        try {
            if (preprocessingService.containsWindow(query)) {
                handler.fail(QueryError.UNSUPPORTED_QUERY);
            } else {
                query = preprocessingService.convertIfBQL(query);
                String id = Utils.getNewQueryID();
                handlerService.addHandler(id, handler);
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
        if (preprocessingService.queryLimitReached()) {
            handler.fail(QueryError.TOO_MANY_QUERIES);
            return sseEmitter;
        }
        try {
            query = preprocessingService.convertIfBQL(query);
            handlerService.addHandler(id, handler);
            queryService.submit(id, query);
        } catch (BQLException e) {
            handler.fail(new BQLError(e));
        } catch (Exception e) {
            handler.fail(QueryError.INVALID_QUERY);
        }
        return sseEmitter;
    }

    @PostMapping(value = "${bullet.endpoint.async}", consumes = { MediaType.APPLICATION_JSON_VALUE }, produces = { MediaType.APPLICATION_JSON_VALUE })
    public CompletableFuture<ResponseEntity<Object>> submitAsyncQuery(@RequestBody AsyncQuery asyncQuery) {
        final String key = asyncQuery.getKey();
        if (key == null || key.isEmpty()) {
            return failWith(QueryError.MISSING_KEY, HttpStatus.BAD_REQUEST);
        }
        if (!statusService.isBackendStatusOk()) {
            return failWith(unavailable());
        }
        if (preprocessingService.queryLimitReached()) {
            return failWith(QueryError.TOO_MANY_QUERIES, HttpStatus.SERVICE_UNAVAILABLE);
        }
        try {
            final String query = preprocessingService.convertIfBQL(asyncQuery.getQuery());
            final String id = Utils.getNewQueryID();
            log.debug("Submitting querying {}", id);
            return queryService.submit(id, query)
                               .thenCompose(b -> createQueryResponse(b, key, id, query))
                               .exceptionally(HTTPQueryController::unavailable);
        } catch (BQLException e) {
            return failWith(new BQLError(e), HttpStatus.BAD_REQUEST);
        } catch (Exception e) {
            return failWith(QueryError.INVALID_QUERY, HttpStatus.BAD_REQUEST);
        }
    }

    @DeleteMapping(path = "${bullet.endpoint.async}/{id}")
    public CompletableFuture<ResponseEntity<Object>> deleteAsyncQuery(@PathVariable String id) {
        log.debug("Delete requested for id: {}", id);
        if (!statusService.isBackendStatusOk()) {
            return failWith(unavailable());
        }
        try {
            log.debug("Removing query {}", id);
            return queryService.kill(id)
                               .thenApply(u -> createResponse(HttpStatus.OK))
                               .exceptionally(HTTPQueryController::unavailable);
        } catch (Exception e) {
            log.error("Error", e);
            return failWith(unavailable());
        }
    }

    private static CompletableFuture<ResponseEntity<Object>> createQueryResponse(boolean status, String key, String id, String query) {
        if (!status) {
            log.error("Unable to create response for id: {}, key: {}, query: {}", id, key, query);
            return failWith(unavailable());
        }
        log.debug("Creating response for id: {}", id);
        return completedFuture(createResponse(HttpStatus.CREATED, new QueryResponse(key, id, query, System.currentTimeMillis())));
    }

    private static CompletableFuture<ResponseEntity<Object>> failWith(QueryError error, HttpStatus status) {
        return failWith(createResponse(status, error));
    }

    private static CompletableFuture<ResponseEntity<Object>> failWith(ResponseEntity<Object> object) {
        return completedFuture(object);
    }

    private static <T> ResponseEntity<T> createResponse(HttpStatus status) {
        return new ResponseEntity<>(status);
    }

    private static <T> ResponseEntity<T> createResponse(HttpStatus status, T object) {
        return new ResponseEntity<>(object, status);
    }

    private static ResponseEntity<Object> unavailable(Throwable throwable) {
        log.error("Exception", throwable);
        return unavailable();
    }

    private static ResponseEntity<Object> unavailable() {
        return new ResponseEntity<>(QueryError.SERVICE_UNAVAILABLE, HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
