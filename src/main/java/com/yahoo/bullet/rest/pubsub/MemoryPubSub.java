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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MemoryPubSub extends PubSub {

    public MemoryPubSub(BulletConfig config) throws PubSubException {
        super(config);
        //this.config = new MemoryPubSubConfig(config);
    }

    @Override
    public Publisher getPublisher() throws PubSubException {
        return new MemoryPublisher(config, context);
    }

    @Override
    public List<Publisher> getPublishers(int n) throws PubSubException {
        // Kafka Publishers are thread safe and can be reused
        return Collections.nCopies(n, getPublisher());
    }

    @Override
    public Subscriber getSubscriber() throws PubSubException {
        // return getSubscriber(partitions, topic);

        if (context == Context.QUERY_PROCESSING) {
            return new MemoryQuerySubscriber();
        }
        return new MemoryResponseSubscriber();
    }

    @Override
    public List<Subscriber> getSubscribers(int n) throws PubSubException {
        List<Subscriber> subscribers = new ArrayList<>();
        if (context == Context.QUERY_PROCESSING) {
            for (int i = 0; i < n; i++) {
                subscribers.add(new MemoryQuerySubscriber());
            }
        } else {
            for (int i = 0; i < n; i++) {
                subscribers.add(new MemoryResponseSubscriber());
            }
        }
        return subscribers;
    }

}
