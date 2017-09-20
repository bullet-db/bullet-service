/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.utils;

import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.rest.resource.QueryError;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata;
import lombok.Getter;

import javax.ws.rs.core.Response;

public abstract class QueryHandler {
    @Getter
    protected boolean complete = false;

    /**
     * Send a {@link PubSubMessage} to the query handler.
     *
     * @param message The PubSubMessage containing a response.
     */
    public abstract void send(PubSubMessage message);

    /**
     * Complete the query. The default implementation sets the complete bit. All overrides should do the same.
     */
    public void complete() {
        complete = true;
    };

    /**
     * Fail the query.
     *
     * @param cause is the {@link QueryError} that caused the fail to be invoked.
     */
    public abstract void fail(QueryError cause);

    /**
     * Indicate that the query was received and accepted by the QUERY_PROCESSING system.
     */
    public abstract void acknowledge();

    /**
     * Get a {@link Response} object with the correct error message and status.
     *
     * @param cause is the {@link QueryError} that caused the fail to be invoked.
     * @return Response with error status and the error message corresponding to cause.
     */
    public Response getErrorResponse(QueryError cause) {
        Clip responseEntity = Clip.of(Metadata.of(Error.makeError(cause.getError(), cause.getResolution())));
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(responseEntity.asJSON()).build();
    }

    /**
     * Convenience method that fails a query with a generic service unavailable error.
     */
    public void fail() {
        fail(QueryError.SERVICE_UNAVAILABLE);
    }
}
