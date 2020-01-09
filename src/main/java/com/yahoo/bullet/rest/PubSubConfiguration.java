/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubResponder;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import com.yahoo.bullet.rest.service.HandlerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Configuration @Slf4j
public class PubSubConfiguration {
    /**
     * Creates a synchronous {@link PubSubResponder} instance.
     *
     * @param handlerService The {@link HandlerService} to use.
     * @return A responder to use for synchronous queries.
     */
    @Bean
    public PubSubResponder syncResponder(HandlerService handlerService) {
        return handlerService;
    }

    @Bean @ConditionalOnMissingBean
    public List<PubSubResponder> asyncResponders() {
        log.info("Async responders are not configured. Using an empty list for async responders");
        return new ArrayList<>();
    }

    /**
     * Creates the {@link PubSubResponder} instances to use for responding to queries.
     *
     * @param syncResponder The required responder to use for synchronous queries.
     * @param asyncResponders Other responders to use for asynchronous queries. Maybe empty.
     * @return A {@link List} of {@link PubSubResponder} instances.
     */
    @Bean
    public List<PubSubResponder> responders(PubSubResponder syncResponder, List<PubSubResponder> asyncResponders) {
        List<PubSubResponder> responders = new ArrayList<>(asyncResponders);
        responders.add(syncResponder);
        return responders;
    }

    /**
     * Creates a PubSub instance from a provided config.
     *
     * @param config The String path to the config file.
     * @return An instance of the particular {@link PubSub} indicated in the config.
     * @throws PubSubException if there were issues creating the PubSub instance.
     */
    @Bean
    public PubSub pubSub(@Value("${bullet.pubsub.config}") String config) throws PubSubException {
        return PubSub.from(new BulletConfig(config));
    }

    /**
     * Creates the specified number of {@link Publisher} instances from the given PubSub.
     *
     * @param pubSub The {@link PubSub} providing the instances.
     * @param publishers The number of publishers to create using {@link PubSub#getPublishers(int)}.
     * @return A {@link List} of {@link Publisher} provided by the {@link PubSub} instance.
     * @throws PubSubException if there were issues creating the instances.
     */
    @Bean
    public List<Publisher> publishers(PubSub pubSub, @Value("${bullet.pubsub.publishers}") int publishers) throws PubSubException {
        return pubSub.getPublishers(publishers);
    }

    /**
     * Creates the specified number of {@link Subscriber} instances from the given PubSub.
     *
     * @param pubSub The {@link PubSub} providing the instances.
     * @param subscribers The number of subscribers to create using {@link PubSub#getSubscribers(int)}.
     * @return A {@link List} of {@link Subscriber} provided by the {@link PubSub} instance.
     * @throws PubSubException if there were issues creating the instances.
     */
    @Bean
    public List<Subscriber> subscribers(PubSub pubSub, @Value("${bullet.pubsub.subscribers}") int subscribers) throws PubSubException {
        return pubSub.getSubscribers(subscribers);
    }
}
