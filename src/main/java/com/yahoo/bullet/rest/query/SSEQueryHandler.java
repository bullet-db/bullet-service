/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.query;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.rest.service.QueryService;
import lombok.AllArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * Query handler that implements results for SSE - multiple results per query.
 */
@AllArgsConstructor
public class SSEQueryHandler extends QueryHandler {
    private String queryID;
    private SseEmitter emitter;
    private QueryService queryService;

    @Override
    public void complete() {
        super.complete();
        emitter.complete();
    }

    @Override
    public void send(PubSubMessage response) {
        if (!isComplete()) {
            try {
                emitter.send(response.getContent(), MediaType.APPLICATION_JSON);
            } catch (Exception e) {
                queryService.submitSignal(queryID, Metadata.Signal.KILL);
                complete();
            }
        }
    }

    @Override
    public void fail(QueryError cause) {
        if (!isComplete()) {
            try {
                emitter.send(cause.toString(), MediaType.APPLICATION_JSON);
            } catch (Exception e) {
                queryService.submitSignal(queryID, Metadata.Signal.KILL);
            }
            complete();
        }
    }
}
