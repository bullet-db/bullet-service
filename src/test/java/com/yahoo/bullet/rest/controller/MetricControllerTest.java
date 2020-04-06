/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.common.metrics.MetricCollector;
import com.yahoo.bullet.common.metrics.MetricPublisher;
import com.yahoo.bullet.rest.common.Metric;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.yahoo.bullet.TestHelpers.assertNoMetric;
import static com.yahoo.bullet.TestHelpers.assertOnlyMetricEquals;
import static java.util.Collections.emptyList;
import static org.mockito.Mockito.mock;

public class MetricControllerTest {
    private static class TestMetricController extends MetricController {
        private TestMetricController(MetricPublisher metricPublisher, MetricCollector metricCollector) {
            super(metricPublisher, metricCollector);
        }
    }

    @Test
    public void testDisablingMetrics() {
        MetricController controller = new TestMetricController(null, null);
        controller.incrementMetric("prefix." + Metric.COUNT);
        controller.incrementMetric("prefix.", Metric.UNAVAILABLE);
        controller.publishMetrics();
        Assert.assertFalse(controller.isMetricEnabled());
        Assert.assertNull(controller.getMetricPublisher());
        Assert.assertNull(controller.getMetricCollector());
    }

    @Test
    public void testIncrementingMetric() {
        MetricCollector collector = new MetricCollector(emptyList());
        MetricController controller = new TestMetricController(mock(MetricPublisher.class), collector);
        Assert.assertNotNull(controller.getMetricPublisher());
        Assert.assertSame(controller.getMetricCollector(), collector);
        Assert.assertTrue(controller.isMetricEnabled());
        controller.incrementMetric("prefix.", Metric.COUNT);
        controller.incrementMetric("prefix." + Metric.COUNT);
        assertOnlyMetricEquals(collector, "prefix." + Metric.COUNT, 2L);
    }
}
