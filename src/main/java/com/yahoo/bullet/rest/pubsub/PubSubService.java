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

    /**
     * Get the next query from the query queue. Queries are removed after being read once.
     *
     * @return The next query.
     */
    public String getQuery() {
        return getFromQueue(queries);
    }

    /**
     * Get the next result from the result queue. Results are removed after being read once.
     *
     * @return the next result.
     */
    public String getResult() {
        return getFromQueue(results);
    }

    /**
     * Add a result to the result queue.
     *
     * @param result The result to add to the result queue.
     */
    public void postResult(String result) {
        addToQueue(results, result);
    }

    /**
     * Add a query to the query queue.
     *
     * @param query The query to add to the queue.
     */
    public void postQuery(String query) {
        addToQueue(queries, query);
    }

    private String getFromQueue(ConcurrentLinkedQueue<PubSubMessage> queue) {
        return queue.isEmpty() ? null : queue.poll().asJSON();
    }

    private void addToQueue(ConcurrentLinkedQueue<PubSubMessage> queue, String message) {
        queue.add(PubSubMessage.fromJSON(message));
    }
}