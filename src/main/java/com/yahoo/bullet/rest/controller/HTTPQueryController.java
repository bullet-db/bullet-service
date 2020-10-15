/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.bql.BQLResult;
import com.yahoo.bullet.common.metrics.MetricCollector;
import com.yahoo.bullet.common.metrics.MetricPublisher;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.rest.common.Metric;
import com.yahoo.bullet.rest.common.Utils;
import com.yahoo.bullet.rest.model.QueryResponse;
import com.yahoo.bullet.rest.query.HTTPQueryHandler;
import com.yahoo.bullet.rest.query.QueryError;
import com.yahoo.bullet.rest.query.SSEQueryHandler;
import com.yahoo.bullet.rest.service.HandlerService;
import com.yahoo.bullet.rest.service.BQLService;
import com.yahoo.bullet.rest.service.QueryService;
import com.yahoo.bullet.rest.service.StatusService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;

@RestController @Slf4j
public class HTTPQueryController extends MetricController {
    private QueryService queryService;
    private HandlerService handlerService;
    private BQLService bqlService;
    private StatusService statusService;

    static final String STATUS_PREFIX = "api.http.status.code.";
    private static final List<String> STATUSES =
        toMetric(STATUS_PREFIX, Metric.OK, Metric.CREATED, Metric.NO_CONTENT, Metric.BAD_REQUEST,
                 Metric.UNPROCESSABLE_ENTITY, Metric.TOO_MANY_REQUESTS, Metric.ERROR, Metric.UNAVAILABLE);

    /**
     * Constructor that takes various services.
     *
     * @param handlerService The {@link HandlerService} to use.
     * @param queryService The {@link QueryService} to use.
     * @param bqlService The {@link BQLService} to use.
     * @param statusService The {@link StatusService} to use.
     * @param metricPublisher The {@link MetricPublisher} to use. It can be null.
     */
    @Autowired
    public HTTPQueryController(HandlerService handlerService, QueryService queryService,
                               BQLService bqlService, StatusService statusService,
                               MetricPublisher metricPublisher) {
        super(metricPublisher, new MetricCollector(STATUSES));
        this.handlerService = handlerService;
        this.queryService = queryService;
        this.bqlService = bqlService;
        this.statusService = statusService;
    }

    /**
     * The method that handles POSTed queries to this endpoint and validates them. Consumes the HTTP request
     * register and transmit the query to Bullet.
     *
     * @param query The String query to submit.
     * @return A {@link CompletableFuture} representing the eventual result.
     */
    @PostMapping(path = "${bullet.endpoint.validate}", consumes = { MediaType.TEXT_PLAIN_VALUE }, produces = { MediaType.APPLICATION_JSON_VALUE })
    public ResponseEntity<Object> validateQuery(@RequestBody String query) {
        BQLResult result = bqlService.toQuery(query);
        if (result.hasErrors()) {
            return respondWith(Metric.UNPROCESSABLE_ENTITY, new QueryError(result.getErrors()).toString());
        }
        return respondWith(Metric.NO_CONTENT, null);
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
        if (!statusService.isBackendStatusOK()) {
            handler.fail(QueryError.SERVICE_UNAVAILABLE);
            return returnWith(Metric.UNAVAILABLE, handler.getResult());
        }
        if (statusService.queryLimitReached()) {
            handler.fail(QueryError.TOO_MANY_QUERIES);
            return returnWith(Metric.TOO_MANY_REQUESTS, handler.getResult());
        }
        BQLResult result = bqlService.toQuery(query);
        if (result.hasErrors()) {
            handler.fail(new QueryError(result.getErrors()));
            return returnWith(Metric.BAD_REQUEST, handler.getResult());
        }
        Query bulletQuery = result.getQuery();
        if (bulletQuery.getWindow().getType() != null) {
            handler.fail(QueryError.UNSUPPORTED_QUERY);
            return returnWith(Metric.BAD_REQUEST, handler.getResult());
        }
        String id = Utils.getNewQueryID();
        log.debug("Submitting HTTP query {}: {}", id, query);
        handlerService.addHandler(id, handler);
        queryService.submit(id, bulletQuery, result.getBql());
        return returnWith(Metric.CREATED, handler.getResult());
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
        if (!statusService.isBackendStatusOK()) {
            handler.fail(QueryError.SERVICE_UNAVAILABLE);
            return returnWith(Metric.UNAVAILABLE, sseEmitter);
        }
        if (statusService.queryLimitReached()) {
            handler.fail(QueryError.TOO_MANY_QUERIES);
            return returnWith(Metric.TOO_MANY_REQUESTS, sseEmitter);
        }
        BQLResult result = bqlService.toQuery(query);
        if (result.hasErrors()) {
            handler.fail(new QueryError(result.getErrors()));
            return returnWith(Metric.BAD_REQUEST, sseEmitter);
        }
        log.debug("Submitting SSE query {}: {}", id, query);
        handlerService.addHandler(id, handler);
        queryService.submit(id, result.getQuery(), result.getBql());
        return returnWith(Metric.CREATED, sseEmitter);
    }

    /**
     * This method handles POSTs for asynchronous queries to the API. These queries do not wait around for the results.
     *
     * @param asyncQuery The String query to submit.
     * @return A {@link CompletableFuture} that resolves to either a {@link QueryResponse} or a {@link QueryError}.
     */
    @PostMapping(value = "${bullet.endpoint.async}", consumes = { MediaType.TEXT_PLAIN_VALUE }, produces = { MediaType.APPLICATION_JSON_VALUE })
    public CompletableFuture<ResponseEntity<Object>> submitAsyncQuery(@RequestBody String asyncQuery) {
        if (!statusService.isBackendStatusOK()) {
            return failWith(unavailable());
        }
        BQLResult result = bqlService.toQuery(asyncQuery);
        if (result.hasErrors()) {
            return failWith(new QueryError(result.getErrors()));
        }
        final String id = Utils.getNewQueryID();
        log.debug("Submitting Async query {}: {}", id, asyncQuery);
        return queryService.submit(id, result.getQuery(), result.getBql())
                           .thenCompose(message -> createQueryResponse(message, id, asyncQuery))
                           .exceptionally(this::internalError);
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
        if (!statusService.isBackendStatusOK()) {
            return failWith(unavailable());
        }
        try {
            log.debug("Removing Async query {}", id);
            return queryService.kill(id)
                               .thenApply(u -> ok())
                               .exceptionally(this::internalError);
        } catch (Exception e) {
            return failWith(internalError(e));
        }
    }

    private CompletableFuture<ResponseEntity<Object>> createQueryResponse(PubSubMessage message, String id, String query) {
        if (message == null) {
            log.error("Unable to create response for id: {}, query: {}", id, query);
            return failWith(internalError(new RuntimeException("Unable to create query")));
        }
        log.debug("Creating response for id: {}", id);
        return completedFuture(respondWith(Metric.CREATED, new QueryResponse(id, query, System.currentTimeMillis())));
    }

    private CompletableFuture<ResponseEntity<Object>> failWith(ResponseEntity<Object> error) {
        return completedFuture(error);
    }

    private CompletableFuture<ResponseEntity<Object>> failWith(QueryError error) {
        return failWith(respondWith(Metric.BAD_REQUEST, error));
    }

    private ResponseEntity<Object> unavailable() {
        return respondWith(Metric.UNAVAILABLE, QueryError.SERVICE_UNAVAILABLE);
    }

    private ResponseEntity<Object> internalError(Throwable e) {
        log.error("Error", e);
        return respondWith(Metric.ERROR, QueryError.SERVICE_UNAVAILABLE);
    }

    private ResponseEntity<Object> ok() {
        return respondWith(Metric.OK, null);
    }

    private <T> ResponseEntity<T> respondWith(Metric metric, T object) {
        return returnWith(metric, new ResponseEntity<>(object, metric.toHTTPStatus()));
    }

    private <T> T returnWith(Metric status, T object) {
        incrementMetric(STATUS_PREFIX, status);
        return object;
    }
}
