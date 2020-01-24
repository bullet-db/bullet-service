/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.common.metrics.MetricCollector;
import com.yahoo.bullet.common.metrics.MetricPublisher;
import com.yahoo.bullet.rest.common.Metric;
import org.springframework.scheduling.annotation.Scheduled;

public class MetricController {
    private final boolean enableMetrics;
    private final MetricPublisher metricPublisher;
    private final MetricCollector metricCollector;

    /**
     * Creates a controller that reports to the given {@link MetricPublisher} the various metrics collected in the
     * given {@link MetricCollector}.
     *
     * @param metricPublisher The {@link MetricPublisher} to use to report metrics.
     * @param metricCollector The {@link MetricCollector} to use.
     */
    public MetricController(MetricPublisher metricPublisher, MetricCollector metricCollector) {
        this.enableMetrics = metricPublisher != null;
        this.metricPublisher = metricPublisher;
        this.metricCollector = metricCollector;
    }

    /**
     * Fires and forgets the metrics using the publisher.
     */
    @Scheduled(fixedDelayString = "${bullet.metric.publish.interval.ms}")
    public void publishMetrics() {
        if (enableMetrics) {
            metricPublisher.fire(metricCollector.extractMetrics());
        }
    }

    /**
     * Increments the {@link Metric} with a given prefix to attach to it.
     *
     * @param prefix The prefix to attach to the given metric.
     * @param metric The {@link Metric} name.
     */
    protected void incrementMetric(String prefix, Metric metric) {
        if (this.enableMetrics) {
            metricCollector.increment(prefix + metric.toString());
        }
    }

    /**
     * Increment the given metric.
     *
     * @param metric The String name of the metric.
     */
    protected void incrementMetric(String metric) {
        if (this.enableMetrics) {
            metricCollector.increment(metric);
        }
    }
}
