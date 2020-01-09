/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.rest.common.Utils;
import com.yahoo.bullet.rest.query.QueryError;
import com.yahoo.bullet.rest.query.QueryHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service @Slf4j
public class StatusService implements Runnable {
    static class TickQueryHandler extends QueryHandler {
        private CompletableFuture<Boolean> result = new CompletableFuture<>();
        private long timeout;

        TickQueryHandler(long timeout) {
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
         * @return true if result was sent within timeout; false, otherwise.
         */
        boolean hasResult() {
            try {
                return result.get(timeout, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                complete();
                return false;
            }
        }
    }

    static final String TICK_QUERY = "{'aggregation':{'type':'RAW','size':1},'duration':1}";

    private QueryService queryService;
    private HandlerService handlerService;
    private long period;
    private long retries;
    private long count;
    @Getter
    private boolean backendStatusOk;

    /**
     * Creates an instance with a tick period and number of retries.
     *
     * @param queryService The {@link QueryService} to use.
     * @param handlerService The {@link HandlerService} to use.
     * @param period Rate at which to ping backend in ms.
     * @param retries Number of times ping can fail before backend status is considered not ok.
     * @param enabled Whether this backend status service is enabled or not.
     */
    @Autowired
    public StatusService(QueryService queryService, HandlerService handlerService,
                         @Value("${bullet.backend.status.tick-ms}") long period,
                         @Value("${bullet.backend.status.retries}") long retries,
                         @Value("${bullet.backend.status.enabled}") Boolean enabled) {
        this.queryService = queryService;
        this.handlerService = handlerService;
        this.period = period;
        this.retries = retries;
        this.count = 0;
        this.backendStatusOk = true;

        if (enabled != null && enabled) {
            Executors.newScheduledThreadPool(1).scheduleAtFixedRate(this, period, period, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void run() {
        TickQueryHandler tickQueryHandler = new TickQueryHandler(period);

        String id = Utils.getNewQueryID();
        handlerService.addHandler(id, tickQueryHandler);
        queryService.submit(id, TICK_QUERY);

        if (tickQueryHandler.hasResult()) {
            count = 0;
            backendStatusOk = true;
        } else {
            count++;
            backendStatusOk = count <= retries;
        }
        if (!backendStatusOk) {
            log.error("Backend is not up! Failing all queries and refusing to accept new queries");
            handlerService.failAllHandlers();
        }
    }
}
