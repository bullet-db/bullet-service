/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.query;

import com.yahoo.bullet.pubsub.PubSubMessage;
import lombok.Getter;

@Getter
public abstract class QueryHandler {
    protected boolean complete = false;

    /**
     * Send a {@link PubSubMessage} to the query handler.
     *
     * @param message The PubSubMessage containing a response.
     */
    public abstract void send(PubSubMessage message);

    /**
     * Completes the query and sets the complete flag. All overrides should do the same or call this.
     */
    public void complete() {
        complete = true;
    }

    /**
     * Fail the query.
     *
     * @param cause is the {@link QueryError} that caused the fail to be invoked.
     */
    public abstract void fail(QueryError cause);

    /**
     * Indicate that the query was received and accepted by the QUERY_PROCESSING system. By default, does nothing.
     */
    public void acknowledge() {
    }

    /**
     * Convenience method that fails a query with a generic service unavailable error.
     */
    public void fail() {
        fail(QueryError.SERVICE_UNAVAILABLE);
    }
}
