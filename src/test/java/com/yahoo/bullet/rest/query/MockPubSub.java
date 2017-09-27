/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.query;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import java.util.Collections;
import java.util.List;

public class MockPubSub extends PubSub {
    public MockPubSub(BulletConfig config) throws PubSubException {
       super(config);
    }

    @Override
    public Publisher getPublisher() {
        return null;
    }

    @Override
    public Subscriber getSubscriber() {
        return null;
    }

    public List<Subscriber> getSubscribers(int n) {
        return Collections.emptyList();
    }

    public List<Publisher> getPublishers(int n) {
        return Collections.emptyList();
    }
}
