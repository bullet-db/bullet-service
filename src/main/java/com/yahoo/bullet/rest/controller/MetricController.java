/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.common.metrics.MetricCollector;
import com.yahoo.bullet.common.metrics.MetricPublisher;
import com.yahoo.bullet.rest.query.QueryError;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.util.concurrent.CompletableFuture.completedFuture;

@Slf4j
abstract class MetricController {
    @Getter(AccessLevel.PACKAGE)
    private final MetricCollector statusCounts;
    private final String metricPrefix;
    private final boolean isEnabled;
    protected final MetricPublisher metricPublisher;

    /**
     * Creates a controller that reports to the given {@link MetricPublisher} the various status of each resource
     * accessed and responded to using the methods herein.
     *
     * @param metricPublisher The {@link MetricPublisher} to use to report metrics.
     * @param metricPrefix The prefix to use for the statuses.
     * @param statuses The initial statuses to use to setup the metrics.
     */
    MetricController(MetricPublisher metricPublisher, String metricPrefix, List<HttpStatus> statuses) {
        this.isEnabled = metricPublisher != null;
        this.metricPublisher = metricPublisher;
        this.metricPrefix = metricPrefix;
        this.statusCounts = isEnabled ? new MetricCollector(statuses.stream().map(this::toMetric).collect(Collectors.toList())) : null;
    }

    /**
     * Fires and forgets the status metrics using the publisher.
     */
    public void reportMetrics() {
        if (isEnabled) {
            metricPublisher.fire(statusCounts.extractMetrics());
        }
    }

    /**
     * Creates a {@link CompletableFuture} that wraps {@link ResponseEntity} with the given {@link HttpStatus} and
     * {@link QueryError}.
     *
     * @param error The error for the entity.
     * @param status The status for the entity.
     * @return A {@link ResponseEntity} with the error body.
     */
    CompletableFuture<ResponseEntity<Object>> failWith(QueryError error, HttpStatus status) {
        return failWith(respondWith(status, error));
    }

    /**
     * Creates a {@link CompletableFuture} wrapping an error {@link ResponseEntity}.
     *
     * @param error The {@link ResponseEntity} representing an error.
     * @return A {@link ResponseEntity} wrapping the error.
     */
    CompletableFuture<ResponseEntity<Object>> failWith(ResponseEntity<Object> error) {
        return completedFuture(error);
    }

    /**
     * Return a {@link ResponseEntity} that represents an unavailable endpoint.
     *
     * @return A {@link ResponseEntity} that represents the service being unavailable.
     */
    ResponseEntity<Object> unavailable() {
        return respondWith(HttpStatus.SERVICE_UNAVAILABLE, QueryError.SERVICE_UNAVAILABLE);
    }

    /**
     * Return a {@link ResponseEntity} that represents an errored endpoint.
     *
     * @return A {@link ResponseEntity} that represents an endpoint having errored.
     */
    ResponseEntity<Object> internalError(Throwable e) {
        log.error("Error", e);
        return respondWith(HttpStatus.INTERNAL_SERVER_ERROR, QueryError.SERVICE_UNAVAILABLE);
    }

    /**
     * Creates a {@link ResponseEntity} with the given {@link HttpStatus}.
     *
     * @param status The status for the entity.
     * @return A {@link ResponseEntity} with no body.
     */
    ResponseEntity<Object> respondWith(HttpStatus status) {
        return respondWith(status, null);
    }

    /**
     * Creates a {@link ResponseEntity} with the given {@link HttpStatus} and body.
     *
     * @param status The status for the entity.
     * @param object The body of the entity
     * @return A created {@link ResponseEntity}.
     */
    <T> ResponseEntity<T> respondWith(HttpStatus status, T object) {
        return returnWith(status, new ResponseEntity<>(object, status));
    }

    /**
     * Return the provided input after handling metrics for the given status.
     *
     * @param status The status for the response.
     * @param object The object being returned as the response.
     * @param <T> The type of the object.
     * @return The object provided as input.
     */
    <T> T returnWith(HttpStatus status, T object) {
        incrementStatus(status);
        return object;
    }

    /**
     * Manages metrics for the given status.
     *
     * @param status The {@link HttpStatus} to increment.
     */
    protected void incrementStatus(HttpStatus status) {
        if (isEnabled) {
            statusCounts.increment(toMetric(status));
        }
    }

    private String toMetric(HttpStatus status) {
        return metricPrefix + status.toString();
    }
}

