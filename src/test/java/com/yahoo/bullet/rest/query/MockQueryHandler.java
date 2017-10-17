/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.query;

import com.yahoo.bullet.pubsub.PubSubMessage;
import lombok.Getter;

@Getter
public class MockQueryHandler extends QueryHandler {
    private QueryError error;

    @Override
    public void send(PubSubMessage message) {
    }

    @Override
    public void fail(QueryError cause) {
        error = cause;
    }

    @Override
    public void acknowledge() {
    }
}
