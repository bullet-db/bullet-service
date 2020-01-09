/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.common;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Mockito.mock;

@Getter
public class MockPubSub extends PubSub {
    protected Context context;
    protected BulletConfig config;
    private List<Integer> publishersAskedFor = new ArrayList<>();
    private List<Integer> subscribersAskedFor = new ArrayList<>();

    public MockPubSub(BulletConfig config) throws PubSubException {
        super(config);
        this.config = config;
    }

    @Override
    public Publisher getPublisher() {
        return mock(Publisher.class);
    }

    @Override
    public Subscriber getSubscriber() {
        return mock(Subscriber.class);
    }

    public List<Subscriber> getSubscribers(int n) {
        subscribersAskedFor.add(n);
        return IntStream.range(0, n).mapToObj(i -> this.getSubscriber()).collect(Collectors.toList());
    }

    public List<Publisher> getPublishers(int n) {
        publishersAskedFor.add(n);
        return IntStream.range(0, n).mapToObj(i -> this.getPublisher()).collect(Collectors.toList());
    }
}
