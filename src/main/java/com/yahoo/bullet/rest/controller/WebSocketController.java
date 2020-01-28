/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.common.metrics.MetricCollector;
import com.yahoo.bullet.common.metrics.MetricPublisher;
import com.yahoo.bullet.rest.common.BQLException;
import com.yahoo.bullet.rest.common.Metric;
import com.yahoo.bullet.rest.common.Utils;
import com.yahoo.bullet.rest.model.BQLError;
import com.yahoo.bullet.rest.model.WebSocketRequest;
import com.yahoo.bullet.rest.query.QueryError;
import com.yahoo.bullet.rest.query.WebSocketQueryHandler;
import com.yahoo.bullet.rest.service.PreprocessingService;
import com.yahoo.bullet.rest.service.StatusService;
import com.yahoo.bullet.rest.service.WebSocketService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.stereotype.Controller;

import java.util.List;

@Controller @Slf4j
public class WebSocketController extends MetricController {
    private WebSocketService webSocketService;
    private PreprocessingService preprocessingService;
    private StatusService statusService;

    static final String STATUS_PREFIX = "api.websocket.status.code.";
    private static final List<String> STATUSES =
        toMetric(STATUS_PREFIX, Metric.CREATED, Metric.BAD_REQUEST, Metric.TOO_MANY_REQUESTS, Metric.UNAVAILABLE);

    /**
     * Constructor that takes various services.
     *
     * @param webSocketService The {@link WebSocketService} to use.
     * @param preprocessingService The {@link PreprocessingService} to use.
     * @param statusService The {@link StatusService} to use.
     * @param metricPublisher The {@link MetricPublisher} to use. It can be null.
     */
    @Autowired
    public WebSocketController(WebSocketService webSocketService, PreprocessingService preprocessingService,
                               StatusService statusService, MetricPublisher metricPublisher) {
        super(metricPublisher, new MetricCollector(STATUSES));
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
            incrementMetric(STATUS_PREFIX, Metric.UNAVAILABLE);
            return;
        }
        try {
            String query = preprocessingService.convertIfBQL(request.getContent());
            if (preprocessingService.queryLimitReached()) {
                queryHandler.fail(QueryError.TOO_MANY_QUERIES);
                incrementMetric(STATUS_PREFIX, Metric.TOO_MANY_REQUESTS);
            } else {
                log.debug("Submitting websocket query {}: {}", queryID, query);
                webSocketService.submitQuery(queryID, sessionID, query, queryHandler);
                incrementMetric(STATUS_PREFIX, Metric.CREATED);
            }
        } catch (BQLException e) {
            queryHandler.fail(new BQLError(e));
            incrementMetric(STATUS_PREFIX, Metric.BAD_REQUEST);
        } catch (Exception e) {
            queryHandler.fail(QueryError.INVALID_QUERY);
            incrementMetric(STATUS_PREFIX, Metric.BAD_REQUEST);
        }
    }

    private void handleKillQuery(WebSocketRequest request, SimpMessageHeaderAccessor headerAccessor) {
        String queryID = request.getContent();
        log.debug("Killing WebSocket query {}", queryID);
        webSocketService.killQuery(headerAccessor.getSessionId(), queryID);
    }
}
