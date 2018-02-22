/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.pubsub;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
public class MemoryPubSub extends PubSub {

    public MemoryPubSub(BulletConfig config) throws PubSubException {
        super(config);
        this.config = new MemoryPubSubConfig(config);
    }

    @Override
    public Publisher getPublisher() throws PubSubException {
        if (context == Context.QUERY_PROCESSING) {
            return new MemoryResponsePublisher(config);
        } else {
            return new MemoryQueryPublisher(config); // Fix this (should be MemoryResponsePublisher)
        }
    }

    @Override
    public List<Publisher> getPublishers(int n) throws PubSubException {
        // Kafka Publishers are thread safe and can be reused
        return Collections.nCopies(n, getPublisher());
    }

    @Override
    public Subscriber getSubscriber() throws PubSubException {
        Number maxUncommittedMessages = config.getAs(MemoryPubSubConfig.MAX_UNCOMMITTED_MESSAGES, Number.class);
        return new MemorySubscriber(config, context, maxUncommittedMessages.intValue());
    }

    @Override
    public List<Subscriber> getSubscribers(int n) throws PubSubException {
        return Collections.nCopies(n, getSubscriber());
    }

}
