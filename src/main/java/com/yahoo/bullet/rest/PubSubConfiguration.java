/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.PubSubResponder;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import com.yahoo.bullet.rest.service.HandlerService;
import com.yahoo.bullet.rest.service.QueryService;
import com.yahoo.bullet.storage.StorageManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.List;

import static com.yahoo.bullet.rest.AsyncConfiguration.ResponderClasses;

@Configuration @Slf4j
public class PubSubConfiguration {
    @Bean @ConditionalOnMissingBean
    public ResponderClasses responderClasses() {
        log.info("Async responder classes are not configured.");
        return null;
    }

    /**
     * Creates a {@link QueryService} instance from various necessary components.
     *
     * @param queryStorageManager The non-null {@link StorageManager} to use.
     * @param handlerService The {@link HandlerService} to use.
     * @param responderClasses The responders to use for asynchronous queries. May be empty.
     * @param publishers The non-empty {@link List} of {@link Publisher} to use.
     * @param subscribers The non-empty {@link List} of {@link Subscriber} to use.
     * @param sleep The time to sleep between checking for messages from the pubsub.
     * @return The created {@link QueryService} instance.
     */
    @Bean
    public QueryService queryService(StorageManager<PubSubMessage> queryStorageManager, HandlerService handlerService,
                                     ResponderClasses responderClasses, List<Publisher> publishers,
                                     List<Subscriber> subscribers, @Value("${bullet.pubsub.sleep-ms}") int sleep) {
        List<PubSubResponder> responders;
        if (responderClasses == null) {
            responders = Collections.singletonList(handlerService);
        } else {
            responders = responderClasses.create();
            responders.add(handlerService);
        }
        return new QueryService(queryStorageManager, responders, publishers, subscribers, sleep);
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
