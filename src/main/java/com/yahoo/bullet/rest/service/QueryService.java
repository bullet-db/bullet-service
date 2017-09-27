/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.RandomPool;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import com.yahoo.bullet.rest.query.PubSubReader;
import com.yahoo.bullet.rest.query.QueryError;
import com.yahoo.bullet.rest.query.QueryHandler;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@Service
public class QueryService {
    @Getter
    private ConcurrentMap<String, QueryHandler> requestQueue;
    private List<PubSubReader> consumers;
    private RandomPool<Publisher> publisherRandomPool;

    /**
     * Creates an instance using a List of Publishers and Subscribers.
     *
     * @param publishers The {@link List} of {@link Publisher} instances for writing queries.
     * @param subscribers The {@link List} of {@link Subscriber} instances for reading results.
     * @param sleepTimeMS The duration to sleep for if a receive from PubSub is empty.
     */
    @Autowired
    public QueryService(List<Publisher> publishers, List<Subscriber> subscribers, @Value("${pubSub.sleepTimeMS}") int sleepTimeMS) {
        Objects.requireNonNull(publishers);
        Objects.requireNonNull(subscribers);
        requestQueue = new ConcurrentHashMap<>();
        publisherRandomPool = new RandomPool<>(publishers);
        consumers = subscribers.stream().map(x -> new PubSubReader(x, requestQueue, sleepTimeMS)).collect(Collectors.toList());
    }

    /**
     * Submit a query to Bullet and register it as a pending request. This is {@link Async}.
     *
     * @param queryID The query ID to register request with.
     * @param query The query to register.
     * @param queryHandler The {@link QueryHandler} object to write responses to.
     */
    @Async
    public void submit(String queryID, String query, QueryHandler queryHandler) {
        Publisher publisher = publisherRandomPool.get();
        try {
            publisher.send(queryID, query);
            requestQueue.put(queryID, queryHandler);
        } catch (Exception e) {
            queryHandler.fail(QueryError.SERVICE_UNAVAILABLE);
        }
    }

    /**
     * Stop all service threads and clear pending requests.
     */
    @PreDestroy
    public void close() {
        consumers.forEach(PubSubReader::close);
        requestQueue.values().forEach(QueryHandler::fail);
        requestQueue.clear();
        publisherRandomPool.clear();
    }
}
