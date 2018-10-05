/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.rest.query.QueryError;
import com.yahoo.bullet.rest.query.QueryHandler;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
public class BackendStatusService implements Runnable {
    private static class TickQueryHandler extends QueryHandler {
        private CompletableFuture<Boolean> result = new CompletableFuture<>();
        private long timeout;

        public TickQueryHandler(long timeout) {
            this.timeout = timeout;
        }

        @Override
        public void send(PubSubMessage message) {
            if (!isComplete()) {
                result.complete(true);
                complete();
            }
        }

        @Override
        public void fail(QueryError cause) {
            if (!isComplete()) {
                result.complete(false);
                complete();
            }
        }

        /**
         * Get whether or not this handler was sent a message.
         *
         * @return true if result was sent within timeout; false, otherwise
         */
        public boolean hasResult() {
            try {
                return result.get(timeout, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                complete();
                return false;
            }
        }
    }

    private long period;
    private long retries;
    private long count;

    @Getter
    private boolean backendStatusOk = true;

    @Autowired
    private QueryService queryService;

    /**
     * Creates an instance with a tick period.
     *
     * @param period Rate at which to check backend status in ms.
     */
    @Autowired
    public BackendStatusService(@Value("${bullet.backend.status.tick-ms}") long period, @Value("${bullet.backend.status.retries") long retries, @Value("${bullet.backend.status.enabled}") Boolean enabled) {
        this.period = period;
        this.retries = retries;
        this.count = 0;

        if (enabled != null && enabled) {
            Executors.newScheduledThreadPool(1).scheduleAtFixedRate(this, period, period, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void run() {
        TickQueryHandler tickQueryHandler = new TickQueryHandler(period);

        queryService.submit(QueryService.getNewQueryID(), "{\"aggregation\":{\"type\":\"RAW\",\"size\":1},\"duration\":1}", tickQueryHandler);

        if (tickQueryHandler.hasResult()) {
            count = 0;
            backendStatusOk = true;
        } else {
            count++;
            backendStatusOk = count >= retries;
        }
        if (!backendStatusOk) {
            queryService.killRunningQueries();
        }
    }
}
