/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.rest.resource.QueryError;
import lombok.AccessLevel;
import lombok.Getter;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;

public class HTTPQueryHandler extends QueryHandler {
    private String content;
    @Getter(AccessLevel.PACKAGE)
    private AsyncResponse asyncResponse;

    /**
     * Create a HTTPQueryHandler with an {@link AsyncResponse}.
     *
     * @param asyncResponse The AsyncResponse object to respond with.
     */
    public HTTPQueryHandler(AsyncResponse asyncResponse) {
        this.asyncResponse = asyncResponse;
    }

    /**
     * Create a HTTPQueryHandler with an {@link AsyncResponse}.
     *
     * @param asyncResponse The AsyncResponse object to respond with.
     * @return a HTTPQueryHandler that responds to asyncResponse.
     */
    public static HTTPQueryHandler of(AsyncResponse asyncResponse) {
        return new HTTPQueryHandler(asyncResponse);
    }

    @Override
    public void send(PubSubMessage message) {
        if (complete) {
            return;
        }
        content = message.getContent();
        Response httpResponse = Response.status(Response.Status.OK).entity(content).build();
        asyncResponse.resume(httpResponse);
    }

    @Override
    public void acknowledge() {
    }

    @Override
    public void fail(QueryError cause) {
        if (!complete) {
            asyncResponse.resume(getErrorResponse(cause));
        }
    }
}
