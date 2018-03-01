/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.pubsub;

import com.yahoo.bullet.pubsub.PubSubMessage;
import org.springframework.stereotype.Service;
import java.util.concurrent.ConcurrentLinkedQueue;

@Service
public class PubSubService {
    private ConcurrentLinkedQueue<PubSubMessage> queries = new ConcurrentLinkedQueue<>();
    private ConcurrentLinkedQueue<PubSubMessage> results = new ConcurrentLinkedQueue<>();

    public String getQuery() {
        return queries.isEmpty() ? null : queries.poll().asJSON();
    }

    public String getResult() {
        return results.isEmpty() ? null : results.poll().asJSON();
    }

    public void postResult(String response) {
        results.add(PubSubMessage.fromJSON(response));
    }

    public void postQuery(String query) {
        queries.add(PubSubMessage.fromJSON(query));
    }
}
