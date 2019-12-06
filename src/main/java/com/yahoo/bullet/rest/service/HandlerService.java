/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.PubSubResponder;
import com.yahoo.bullet.rest.common.Utils;
import com.yahoo.bullet.rest.query.QueryHandler;
import lombok.AccessLevel;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This is used for synchronous sending and handling of queries.
 */
@Component
public class HandlerService implements PubSubResponder {
    // Exposed for testing only.
    @Getter(AccessLevel.PACKAGE)
    private ConcurrentMap<String, QueryHandler> queries;

    /**
     * Constructor that creates a responder.
     */
    @Autowired
    public HandlerService() {
        queries = new ConcurrentHashMap<>();
    }

    @Override
    public void respond(String id, PubSubMessage message) {
        QueryHandler handler = getQuery(id);
        synchronized (handler) {
            if (!handler.isComplete()) {
                handler.send(message);
                if (Utils.isDone(message)) {
                    handler.complete();
                }
                if (handler.isComplete()) {
                    removeQuery(message.getId());
                }
            }
        }
    }

    /**
     * Adds the given {@link QueryHandler} for the given ID to this service.
     *
     * @param id The ID of the query.
     * @param handler The {@link QueryHandler} instance to add.
     */
    public void addQuery(String id, QueryHandler handler) {
        queries.put(id, handler);
    }

    /**
     * Retrieves the {@link QueryHandler} for the given ID, if it exists.
     *
     * @param id The ID of the query.
     * @return The {@link QueryHandler} instance or null if the query does not exist.
     */
    public QueryHandler getQuery(String id) {
        return queries.get(id);
    }

    /**
     * Removes a {@link QueryHandler} for the given ID.
     *
     * @param id The ID of the query.
     * @return The {@link QueryHandler} instance or null if the query does not exist.
     */
    public QueryHandler removeQuery(String id) {
        return queries.remove(id);
    }

    /**
     * Checks to see if a given query exists.
     *
     * @param id The ID of the query.
     * @return A boolean denoting if the query exists in this service.
     */
    public boolean hasQuery(String id) {
        return queries.containsKey(id);
    }

    /**
     * Fail a given query, if it exists. This does not submit anything to Bullet. It simply removes the query and
     * invokes its {@link QueryHandler#fail()} method.
     *
     * @param id The ID of a query to fail, if it exists.
     * @return true if the query was failed.
     */
    public boolean failQuery(String id) {
        QueryHandler handler = queries.remove(id);
        if (handler == null) {
            return false;
        }
        handler.fail();
        return true;
    }

    /**
     * Clears all pending queries. This does not send anything to Bullet.
     */
    public void failAllQueries() {
        queries.values().forEach(QueryHandler::fail);
        queries.clear();
    }

    /**
     * Get the number of running queries.
     *
     * @return The number of running queries.
     */
    public int queryCount() {
        return queries.size();
    }

    /**
     * Stop all service threads and clear pending requests.
     */
    @PreDestroy
    public void close() {
        failAllQueries();
    }
}
