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
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This is used for synchronous sending and handling of queries. To store and manage {@link QueryHandler} instances.
 */
@Service
public class HandlerService extends PubSubResponder {
    // Exposed for testing only.
    @Getter(AccessLevel.PACKAGE)
    private ConcurrentMap<String, QueryHandler> handlers;

    /**
     * Constructor that creates a responder.
     */
    @Autowired
    public HandlerService() {
        super(null);
        handlers = new ConcurrentHashMap<>();
    }

    @Override
    public void respond(String id, PubSubMessage message) {
        QueryHandler handler = getHandler(id);
        synchronized (handler) {
            if (!handler.isComplete()) {
                handler.send(message);
                if (Utils.isDone(message)) {
                    handler.complete();
                }
                if (handler.isComplete()) {
                    removeHandler(message.getId());
                }
            }
        }
    }

    /**
     * Adds the given {@link QueryHandler} for the given ID to this service.
     *
     * @param id The ID of the handler.
     * @param handler The {@link QueryHandler} instance to add.
     */
    public void addHandler(String id, QueryHandler handler) {
        handlers.put(id, handler);
    }

    /**
     * Retrieves the {@link QueryHandler} for the given ID, if it exists.
     *
     * @param id The ID of the handler.
     * @return The {@link QueryHandler} instance or null if the handler does not exist.
     */
    public QueryHandler getHandler(String id) {
        return handlers.get(id);
    }

    /**
     * Removes a {@link QueryHandler} for the given ID.
     *
     * @param id The ID of the handler.
     * @return The {@link QueryHandler} instance or null if the handler does not exist.
     */
    public QueryHandler removeHandler(String id) {
        return handlers.remove(id);
    }

    /**
     * Checks to see if a given handler exists.
     *
     * @param id The ID of the handler.
     * @return A boolean denoting if the handler exists in this service.
     */
    public boolean hasHandler(String id) {
        return handlers.containsKey(id);
    }

    /**
     * Fail a given handler, if it exists. This does not submit anything to Bullet. It simply removes the handler and
     * invokes its {@link QueryHandler#fail()} method.
     *
     * @param id The ID of a handler to fail, if it exists.
     * @return true if the handler was failed.
     */
    public boolean failHandler(String id) {
        QueryHandler handler = handlers.remove(id);
        if (handler == null) {
            return false;
        }
        handler.fail();
        return true;
    }

    /**
     * Clears all pending handlers. This does not send anything to Bullet.
     */
    public void failAllHandlers() {
        handlers.values().forEach(QueryHandler::fail);
        handlers.clear();
    }

    /**
     * Get the number of running handlers.
     *
     * @return The number of running handlers.
     */
    public int count() {
        return handlers.size();
    }

    /**
     * Stop all service threads and clear pending handlers.
     */
    @PreDestroy
    public void close() {
        failAllHandlers();
    }
}
