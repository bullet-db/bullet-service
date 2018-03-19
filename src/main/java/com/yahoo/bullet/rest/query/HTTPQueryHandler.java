/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.query;

import com.yahoo.bullet.pubsub.PubSubMessage;
import lombok.NoArgsConstructor;

import java.util.concurrent.CompletableFuture;

/**
 * Query handler that implements results for HTTP - one result per query. In other words,
 * a single {@link #send(PubSubMessage)} or {@link #fail(QueryError)} call is supported. Use
 * {@link #getResult()} to get a {@link CompletableFuture} that resolves to the single
 * result or error.
 */
@NoArgsConstructor
public class HTTPQueryHandler extends QueryHandler {
    private CompletableFuture<String> result = new CompletableFuture<>();

    @Override
    public void send(PubSubMessage message) {
        if (!isComplete()) {
            result.complete(message.getContent());
            complete();
        }
    }

    @Override
    public void fail(QueryError cause) {
        if (!isComplete()) {
            result.complete(cause.toString());
            complete();
        }
    }

    /**
     * Get the single eventual result sent to this handler.
     *
     * @return The {@link CompletableFuture} of the single result that will eventually (but not guaranteed) to be added.
     */
    public CompletableFuture<String> getResult() {
        return result;
    }
}
