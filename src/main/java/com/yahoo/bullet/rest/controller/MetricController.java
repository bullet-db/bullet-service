/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.common.metrics.MetricCollector;
import com.yahoo.bullet.common.metrics.MetricPublisher;
import com.yahoo.bullet.rest.common.Metrizable;
import lombok.Getter;
import org.springframework.scheduling.annotation.Scheduled;

@Getter
public abstract class MetricController implements Metrizable {
    private final boolean metricEnabled;
    private final MetricPublisher metricPublisher;
    private final MetricCollector metricCollector;

    /**
     * Creates a class that reports to the given {@link MetricPublisher} the various metrics collected in the
     * given {@link MetricCollector}.
     *
     * @param metricPublisher The {@link MetricPublisher} to use to report metrics.
     * @param metricCollector The {@link MetricCollector} to use.
     */
    public MetricController(MetricPublisher metricPublisher, MetricCollector metricCollector) {
        this.metricEnabled = metricPublisher != null;
        this.metricPublisher = metricPublisher;
        this.metricCollector = metricCollector;
    }

    /**
     * Fires and forgets the metrics using the publisher.
     */
    @Scheduled(fixedDelayString = "${bullet.metric.publish.interval.ms}")
    public void publishMetrics() {
        if (metricEnabled) {
            metricPublisher.fire(metricCollector.extractMetrics());
        }
    }
}
