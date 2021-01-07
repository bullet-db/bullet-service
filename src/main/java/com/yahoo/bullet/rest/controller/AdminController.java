/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.common.metrics.MetricCollector;
import com.yahoo.bullet.common.metrics.MetricPublisher;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.rest.common.Metric;
import com.yahoo.bullet.rest.service.QueryService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RestController @Slf4j
public class AdminController extends MetricController {
    private final QueryService queryService;

    static final String STATUS_PREFIX = "admin.http.status.code.";

    private static final List<String> STATUSES = toMetric(STATUS_PREFIX, Metric.OK, Metric.ERROR);

    /**
     * The constructor that takes a {@link QueryService} and a {@link MetricPublisher}.
     *
     * @param queryService The non-null service for dealing with queries.
     * @param metricPublisher The non-null {@link MetricPublisher} for publishing metrics.
     */
    @Autowired
    public AdminController(QueryService queryService, MetricPublisher metricPublisher) {
        super(metricPublisher, new MetricCollector(STATUSES));
        this.queryService = queryService;
    }

    @PatchMapping(path = "${bullet.endpoint.replay}")
    public CompletableFuture<ResponseEntity<Object>> sendReplay() {
        try {
            return queryService.send(UUID.randomUUID().toString(), Metadata.Signal.REPLAY)
                               .thenApply(p -> respondWith(Metric.OK))
                               .exceptionally(e -> respondWith(Metric.ERROR));
        } catch (Exception e) {
            log.error("Error while trying to replay", e);
            return CompletableFuture.completedFuture(respondWith(Metric.ERROR));
        }
    }

    private ResponseEntity<Object> respondWith(Metric metric) {
        incrementMetric(STATUS_PREFIX, metric);
        return new ResponseEntity<>(null, metric.toHTTPStatus());
    }
}
