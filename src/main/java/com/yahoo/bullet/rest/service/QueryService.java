/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.common.RandomPool;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import com.yahoo.bullet.rest.query.PubSubReader;
import com.yahoo.bullet.rest.query.QueryError;
import com.yahoo.bullet.rest.query.QueryHandler;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
    private ConcurrentMap<String, QueryHandler> runningQueries;
    private List<PubSubReader> consumers;
    private RandomPool<Publisher> publisherRandomPool;

    /**
     * Creates an instance using a List of Publishers and Subscribers.
     *
     * @param publishers The {@link List} of {@link Publisher} instances for writing queries.
     * @param subscribers The {@link List} of {@link Subscriber} instances for reading results.
     * @param sleep The duration to sleep for if a receive from PubSub is empty.
     */
    @Autowired
    public QueryService(List<Publisher> publishers, List<Subscriber> subscribers, @Value("${bullet.pubsub.sleep-ms}") int sleep) {
        Objects.requireNonNull(publishers);
        Objects.requireNonNull(subscribers);
        runningQueries = new ConcurrentHashMap<>();
        publisherRandomPool = new RandomPool<>(publishers);
        consumers = subscribers.stream().map(x -> new PubSubReader(x, runningQueries, sleep)).collect(Collectors.toList());
    }

    /**
     * Submit a query to Bullet and register it as a pending request.
     *
     * @param queryID The query ID to register request with.
     * @param query The query to register.
     * @param queryHandler The {@link QueryHandler} object that handles the query.
     */
    public void submit(String queryID, String query, QueryHandler queryHandler) {
        Publisher publisher = publisherRandomPool.get();
        try {
            publisher.send(queryID, query);
            runningQueries.put(queryID, queryHandler);
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
        runningQueries.values().forEach(QueryHandler::fail);
        runningQueries.clear();
        publisherRandomPool.clear();
    }
}
