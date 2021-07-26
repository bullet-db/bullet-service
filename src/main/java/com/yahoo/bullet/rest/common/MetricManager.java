/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.common;

import com.yahoo.bullet.common.metrics.MetricCollector;
import com.yahoo.bullet.common.metrics.MetricPublisher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public interface MetricManager {
    /**
     * Returns true if metrics should be enabled.
     *
     * @return A boolean denoting whether metrics should be published.
     */
    boolean isMetricEnabled();

    /**
     * Returns a {@link MetricPublisher} to use. Only used if {@link #isMetricEnabled()} is true.
     *
     * @return A non-null {@link MetricPublisher} to use.
     */
    MetricPublisher getMetricPublisher();

    /**
     * Returns a {@link MetricCollector} to use. Only used if {@link #isMetricEnabled()} is true.
     *
     * @return A non-null {@link MetricCollector} to use.
     */
    MetricCollector getMetricCollector();

    /**
     * Increments the {@link Metric} with a given prefix to attach to it.
     *
     * @param prefix The prefix to attach to the given metric.
     * @param metric The {@link Metric} name.
     */
    default void incrementMetric(String prefix, Metric metric) {
        if (isMetricEnabled()) {
            // Doesn't call incrementMetric(String) on purpose to avoid the if again
            getMetricCollector().increment(prefix + metric.toString());
        }
    }

    /**
     * Increment the given metric.
     *
     * @param metric The String name of the metric.
     */
    default void incrementMetric(String metric) {
        if (isMetricEnabled()) {
            getMetricCollector().increment(metric);
        }
    }

    /**
     * Concatenates a String prefix to the given metrics.
     *
     * @param prefix The String prefix to add.
     * @param metrics The {@link Metric} varargs.
     * @return A {@link List} of String metrics with the prefix added to each.
     */
    static List<String> toMetric(String prefix, Metric... metrics) {
        return Arrays.stream(metrics).map(Objects::toString).map(prefix::concat).collect(Collectors.toCollection(ArrayList::new));
    }
}
