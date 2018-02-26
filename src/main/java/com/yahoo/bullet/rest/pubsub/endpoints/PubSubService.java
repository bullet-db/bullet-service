/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.pubsub.endpoints;

import com.yahoo.bullet.pubsub.PubSubMessage;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentLinkedQueue;

@Service
public class PubSubService {
    private ConcurrentLinkedQueue<PubSubMessage> queries = new ConcurrentLinkedQueue<>();
    private ConcurrentLinkedQueue<PubSubMessage> responses = new ConcurrentLinkedQueue<>();

    public String readQuery() {
        return queries.isEmpty() ? null : queries.poll().asJSON();
    }

    public String readResponse() {
        return responses.isEmpty() ? null : responses.poll().asJSON();
    }

    public void writeResponse(String response) {
        responses.add(PubSubMessage.fromJSON(response));
    }

    public void writeQuery(String query) {
        queries.add(PubSubMessage.fromJSON(query));
    }
}
