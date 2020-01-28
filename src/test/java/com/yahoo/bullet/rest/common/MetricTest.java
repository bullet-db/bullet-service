/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.common;

import org.springframework.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

public class MetricTest {
    @Test
    public void testGettingStatuses() {
        List<Metric> withStatuses = Arrays.asList(Metric.OK, Metric.CREATED, Metric.BAD_REQUEST, Metric.NOT_FOUND,
                                                  Metric.TOO_MANY_REQUESTS, Metric.ERROR, Metric.UNAVAILABLE);
        List<Metric> withoutStatuses = Arrays.asList(Metric.COUNT, Metric.AVERAGE, Metric.LATENCY);
        for (Metric metric : withStatuses) {
            Assert.assertTrue(Metric.isHTTPStatusCode(metric));
            HttpStatus status = metric.toHTTPStatus();
            Assert.assertNotNull(status);
            Assert.assertEquals(metric.toString(), status.toString());
        }
        for (Metric metric : withoutStatuses) {
            Assert.assertFalse(Metric.isHTTPStatusCode(metric));
            Assert.assertNull(metric.toHTTPStatus());
            Assert.assertNotNull(metric.toString());
        }
    }
}
