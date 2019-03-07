/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.common.RandomPool;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import com.yahoo.bullet.rest.query.PubSubReader;
import com.yahoo.bullet.rest.query.QueryError;
import com.yahoo.bullet.rest.query.QueryHandler;
import lombok.AccessLevel;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@Service
public class QueryService {
    // Exposed for testing only.
    @Getter(AccessLevel.PACKAGE)
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
            queryHandler.acknowledge();
        } catch (Exception e) {
            queryHandler.fail(QueryError.SERVICE_UNAVAILABLE);
        }
    }

    /**
     * Submits a {@link Metadata.Signal} signal to Bullet for the given query ID. This query need not exist.
     *
     * @param queryID The query ID to submit the signal for.
     * @param signal The signal to send.
     */
    public void sendSignal(String queryID, Metadata.Signal signal) {
        sendMessage(new PubSubMessage(queryID, null, new Metadata(signal, null)));
    }

    /**
     * Sends a {@link PubSubMessage} to Bullet.
     *
     * @param message The message to send.
     */
    public void sendMessage(PubSubMessage message) {
        Publisher publisher = publisherRandomPool.get();
        try {
            publisher.send(message);
        } catch (Exception e) {
            // Ignore failure.
        }
    }

    /**
     * Checks to see if a given query exists.
     *
     * @param queryID The ID of the query.
     * @return A boolean denoting if the query exists in this service.
     */
    public boolean hasQuery(String queryID) {
        return runningQueries.containsKey(queryID);
    }

    /**
     * Retrieves the {@link QueryHandler} for the given ID, if it exists.
     *
     * @param queryID The ID of the query.
     * @return The {@link QueryHandler} instance or null if the query does not exist.
     */
    public QueryHandler getQuery(String queryID) {
        return runningQueries.get(queryID);
    }

    /**
     * Removes a {@link QueryHandler} for the given ID.
     *
     * @param queryID The ID of the query.
     * @return The {@link QueryHandler} instance or null if the query does not exist.
     */
    public QueryHandler removeQuery(String queryID) {
        return runningQueries.remove(queryID);
    }

    /**
     * Submits a {@link Metadata.Signal#KILL} signal to Bullet for the given query ID and removes the query. This does
     * not invoke the {@link QueryHandler#fail()} method on the query.
     *
     * @param queryID The query ID to submit the kill signal for.
     * @return The killed query, if it exists.
     */
    public QueryHandler killQuery(String queryID) {
        sendSignal(queryID, Metadata.Signal.KILL);
        return removeQuery(queryID);
    }

    /**
     * Fail a given query, if it exists. This does not submit anything to Bullet. It simply removes the query and
     * invokes its {@link QueryHandler#fail()} method.
     *
     * @param queryID The ID of a query to fail, if it exists.
     * @return true if the query was failed.
     */
    public boolean failQuery(String queryID) {
        QueryHandler handler = runningQueries.remove(queryID);
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
        runningQueries.values().forEach(QueryHandler::fail);
        runningQueries.clear();
    }

    /**
     * Get the number of running queries.
     *
     * @return The number of running queries.
     */
    public int queryCount() {
        return runningQueries.size();
    }

    /**
     * Stop all service threads and clear pending requests.
     */
    @PreDestroy
    public void close() {
        consumers.forEach(PubSubReader::close);
        failAllQueries();
        publisherRandomPool.clear();
    }

    /**
     * Get a new unique query ID.
     *
     * @return A new unique query ID.
     */
    public static String getNewQueryID() {
        return UUID.randomUUID().toString();
    }
}
