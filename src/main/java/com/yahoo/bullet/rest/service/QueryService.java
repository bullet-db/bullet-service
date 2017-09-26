/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.RandomPool;
import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubException;
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
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@Service
public class QueryService {
    @Getter
    private ConcurrentMap<String, QueryHandler> requestQueue;

    private List<PubSubReader> services;
    private RandomPool<Publisher> publisherRandomPool;

    /**
     * Creates an instance using a path to config file for creating a {@link PubSub} and other config parameters.
     *
     * @param config The path to a config file to create a {@link PubSub} that supplies the {@link Subscriber}
     *               and {@link Publisher} instances for the particular PubSub instance.
     * @param numberConsumers The number of threads reading responses from the PubSub.
     * @param numberPublishers The number of Publishers writing queries to the PubSub.
     * @param sleepTimeMS The duration to sleep for if a receive from PubSub is empty.
     * @throws PubSubException if there are any failures.
     * @throws IOException if there are any issues reading the config file.
     */
    @Autowired
    public QueryService(@Value("${pubsub.config}") String config,
                        @Value("${pubsub.consumers}") int numberConsumers,
                        @Value("${pubsub.publishers}") int numberPublishers,
                        @Value("{pubsub.sleepTimeMS}") int sleepTimeMS) throws PubSubException, IOException {
        this(PubSub.from(new BulletConfig(config)), numberConsumers, numberPublishers, sleepTimeMS);
    }

    /**
     * Creates an instance using a {@link PubSub} instance and other config parameters.
     *
     * @param pubSub The {@link PubSub} instance that supplies the {@link Subscriber} and {@link Publisher} instances.
     * @param numberConsumers The number of threads reading responses from the PubSub.
     * @param numberPublishers The number of Publishers writing queries to the PubSub.
     * @param sleepTimeMS The duration to sleep for if a receive from PubSub is empty.
     * @throws PubSubException if there are any failures.
     */
    public QueryService(PubSub pubSub, int numberConsumers, int numberPublishers, int sleepTimeMS) throws PubSubException {
        this.requestQueue = new ConcurrentHashMap<>();
        this.publisherRandomPool = new RandomPool<>(pubSub.getPublishers(numberPublishers));
        // Start threads that read from the PubSub and write to waiting requests.
        List<Subscriber> subscribers = pubSub.getSubscribers(numberConsumers);
        services = subscribers.stream().map(x -> new PubSubReader(x, requestQueue, sleepTimeMS)).collect(Collectors.toList());
    }

    /**
     * Submit a query to Bullet and register it as a pending request.
     *
     * @param queryID The query ID to register request with.
     * @param query The query to register.
     * @param queryHandler The {@link QueryHandler} object to write responses to.
     */
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
        services.forEach(PubSubReader::close);
        requestQueue.values().forEach(QueryHandler::fail);
        requestQueue.clear();
        publisherRandomPool.clear();
    }
}
