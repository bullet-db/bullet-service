/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest;

import com.yahoo.bullet.common.metrics.HTTPMetricEventPublisher;
import com.yahoo.bullet.common.metrics.MetricPublisher;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MetricConfigurationTest {
    @Test
    public void testCreatingPublisher() {
        MetricConfiguration configuration = new MetricConfiguration();
        MetricPublisher publisher;
        publisher = configuration.metricPublisher(false, null);
        Assert.assertNull(publisher);
        publisher = configuration.metricPublisher(true, "metric_defaults.yaml");
        Assert.assertTrue(publisher instanceof HTTPMetricEventPublisher);
    }
}
