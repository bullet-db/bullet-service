/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.common.metrics.MetricPublisher;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.rest.common.BQLException;
import com.yahoo.bullet.rest.common.Utils;
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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;

@RestController @Slf4j
public class HTTPQueryController extends MetricController {
    private QueryService queryService;
    private HandlerService handlerService;
    private PreprocessingService preprocessingService;
    private StatusService statusService;

    static final String STATUS_PREFIX = "api.http.status.code.";
    private static final List<HttpStatus> STATUSES =
            Arrays.asList(HttpStatus.OK, HttpStatus.CREATED, HttpStatus.BAD_REQUEST, HttpStatus.INTERNAL_SERVER_ERROR,
                          HttpStatus.SERVICE_UNAVAILABLE);
    /**
     * Constructor that takes various services.
     *
     * @param handlerService The {@link HandlerService} to use.
     * @param queryService The {@link QueryService} to use.
     * @param preprocessingService The {@link PreprocessingService} to use.
     * @param statusService The {@link StatusService} to use.
     * @param metricPublisher The {@link MetricPublisher} to use. It can be null.
     */
    @Autowired
    public HTTPQueryController(HandlerService handlerService, QueryService queryService,
                               PreprocessingService preprocessingService, StatusService statusService,
                               MetricPublisher metricPublisher) {
        super(metricPublisher, STATUS_PREFIX, STATUSES);
        this.handlerService = handlerService;
        this.queryService = queryService;
        this.preprocessingService = preprocessingService;
        this.statusService = statusService;
    }

    /**
     * The method that handles POSTs to this endpoint. Consumes the HTTP request, invokes {@link HandlerService} to
     * register and transmit the query to Bullet.
     *
     * @param query The String query to submit.
     * @return A {@link CompletableFuture} representing the eventual result.
     */
    @PostMapping(path = "${bullet.endpoint.http}", consumes = { MediaType.TEXT_PLAIN_VALUE }, produces = { MediaType.APPLICATION_JSON_VALUE })
    public CompletableFuture<String> submitHTTPQuery(@RequestBody String query) {
        HTTPQueryHandler handler = new HTTPQueryHandler();
        if (!statusService.isBackendStatusOk()) {
            handler.fail(QueryError.SERVICE_UNAVAILABLE);
            return returnWith(HttpStatus.SERVICE_UNAVAILABLE, handler.getResult());
        }
        if (preprocessingService.queryLimitReached()) {
            handler.fail(QueryError.TOO_MANY_QUERIES);
            return returnWith(HttpStatus.SERVICE_UNAVAILABLE, handler.getResult());
        }
        try {
            if (preprocessingService.containsWindow(query)) {
                handler.fail(QueryError.UNSUPPORTED_QUERY);
                return returnWith(HttpStatus.BAD_REQUEST, handler.getResult());
            } else {
                query = preprocessingService.convertIfBQL(query);
                String id = Utils.getNewQueryID();
                log.debug("Submitting HTTP query {}: {}", id, query);
                handlerService.addHandler(id, handler);
                queryService.submit(id, query);
                return returnWith(HttpStatus.OK, handler.getResult());
            }
        } catch (BQLException e) {
            handler.fail(new BQLError(e));
            return returnWith(HttpStatus.BAD_REQUEST, handler.getResult());
        } catch (Exception e) {
            handler.fail(QueryError.INVALID_QUERY);
            return returnWith(HttpStatus.BAD_REQUEST, handler.getResult());
        }
    }

    /**
     * The method that handles SSE POSTs to this endpoint. Consumes the HTTP request, invokes {@link HandlerService} to
     * register and transmit the query to Bullet.
     *
     * @param query The String query to submit.
     * @return A {@link SseEmitter} to send streaming results.
     */
    @PostMapping(value = "${bullet.endpoint.sse}", consumes = { MediaType.TEXT_PLAIN_VALUE }, produces = { MediaType.APPLICATION_JSON_VALUE })
    public SseEmitter submitSSEQuery(@RequestBody String query) {
        SseEmitter sseEmitter = new SseEmitter();
        String id = Utils.getNewQueryID();
        SSEQueryHandler handler = new SSEQueryHandler(id, sseEmitter, queryService);
        if (!statusService.isBackendStatusOk()) {
            handler.fail(QueryError.SERVICE_UNAVAILABLE);
            return returnWith(HttpStatus.SERVICE_UNAVAILABLE, sseEmitter);
        }
        if (preprocessingService.queryLimitReached()) {
            handler.fail(QueryError.TOO_MANY_QUERIES);
            return returnWith(HttpStatus.SERVICE_UNAVAILABLE, sseEmitter);
        }
        try {
            query = preprocessingService.convertIfBQL(query);
            log.debug("Submitting SSE query {}: {}", id, query);
            handlerService.addHandler(id, handler);
            queryService.submit(id, query);
            return returnWith(HttpStatus.OK, sseEmitter);
        } catch (BQLException e) {
            handler.fail(new BQLError(e));
            return returnWith(HttpStatus.BAD_REQUEST, sseEmitter);
        } catch (Exception e) {
            handler.fail(QueryError.INVALID_QUERY);
            return returnWith(HttpStatus.BAD_REQUEST, sseEmitter);
        }
    }

    /**
     * This method handles POSTs for asynchronous queries to the API. These queries do not wait around for the results.
     *
     * @param asyncQuery The String query to submit.
     * @return A {@link CompletableFuture} that resolves to either a {@link QueryResponse} or a {@link QueryError}.
     */
    @PostMapping(value = "${bullet.endpoint.async}", consumes = { MediaType.TEXT_PLAIN_VALUE }, produces = { MediaType.APPLICATION_JSON_VALUE })
    public CompletableFuture<ResponseEntity<Object>> submitAsyncQuery(@RequestBody String asyncQuery) {
        if (!statusService.isBackendStatusOk()) {
            return failWith(unavailable());
        }
        try {
            final String query = preprocessingService.convertIfBQL(asyncQuery);
            final String id = Utils.getNewQueryID();
            log.debug("Submitting Async query {}: {}", id, query);
            return queryService.submit(id, query)
                               .thenCompose(message -> createQueryResponse(message, id, query))
                               .exceptionally(this::internalError);
        } catch (BQLException e) {
            return failWith(new BQLError(e), HttpStatus.BAD_REQUEST);
        } catch (Exception e) {
            return failWith(QueryError.INVALID_QUERY, HttpStatus.BAD_REQUEST);
        }
    }

    /**
     * This method handles DELETEs for any asynchronous queries submitted to the API.
     *
     * @param id The ID returned in the {@link QueryResponse} from the previous submit call signifying the query to delete.
     * @return {@link CompletableFuture} that resolves to a 200 if the delete was successful or the appropriate code otherwise.
     */
    @DeleteMapping(path = "${bullet.endpoint.async}/{id}")
    public CompletableFuture<ResponseEntity<Object>> deleteAsyncQuery(@PathVariable String id) {
        log.debug("Delete requested for id: {}", id);
        if (!statusService.isBackendStatusOk()) {
            return failWith(unavailable());
        }
        try {
            log.debug("Removing Async query {}", id);
            return queryService.kill(id)
                               .thenApply(u -> respondWith(HttpStatus.OK))
                               .exceptionally(this::internalError);
        } catch (Exception e) {
            return failWith(internalError(e));
        }
    }

    private CompletableFuture<ResponseEntity<Object>> createQueryResponse(PubSubMessage message, String id, String query) {
        if (message == null) {
            log.error("Unable to create response for id: {}, query: {}", id, query);
            return failWith(unavailable());
        }
        log.debug("Creating response for id: {}", id);
        return completedFuture(respondWith(HttpStatus.CREATED, new QueryResponse(id, query, System.currentTimeMillis())));
    }
}
