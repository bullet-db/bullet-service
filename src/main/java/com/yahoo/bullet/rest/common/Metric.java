/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.common;

import org.springframework.http.HttpStatus;

/**
 * This enum encapsulates the various metrics and statuses that various endpoints served by the API can have.
 * The {@link #toHTTPStatus()} returns the HTTP status code equivalent of each if {@link #isHTTPStatusCode(Metric)} is
 * true for it.
 */
public enum Metric {
    OK(HttpStatus.OK),
    CREATED(HttpStatus.CREATED),
    BAD_REQUEST(HttpStatus.BAD_REQUEST),
    NOT_FOUND(HttpStatus.NOT_FOUND),
    TOO_MANY_REQUESTS(HttpStatus.TOO_MANY_REQUESTS),
    ERROR(HttpStatus.INTERNAL_SERVER_ERROR),
    UNAVAILABLE(HttpStatus.SERVICE_UNAVAILABLE),
    COUNT("count"),
    AVERAGE("average"),
    LATENCY("latency");

    private HttpStatus status;
    private String name;

    Metric(HttpStatus status) {
        this.status = status;
    }

    Metric(String name) {
        this.name = name;
    }

    /**
     * The {@link HttpStatus} equivalent of this enum, if one exists.
     *
     * @return The status code of the HTTP equivalent of this or null if none exists.
     */
    public HttpStatus toHTTPStatus() {
        return status;
    }

    @Override
    public String toString() {
        return status == null ? name : status.toString();
    }

    /**
     * Returns if the given metric corresponds to a HTTP status.
     *
     * @param metric The metric to check. Must be non-null.
     * @return A boolean denoting if the metric is a HTTP status.
     */
    public static boolean isHTTPStatusCode(Metric metric) {
        return metric.status != null;
    }
}
