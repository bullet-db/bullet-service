/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.common;

import com.yahoo.bullet.common.RandomPool;
import com.yahoo.bullet.pubsub.Publisher;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class PublisherRandomPool extends RandomPool<Publisher> implements AutoCloseable {
    private List<Publisher> publishers;

    /**
     * Constructor for the RandomPool that takes a list of items.
     *
     * @param items A list of items to form the pool with.
     */
    public PublisherRandomPool(List<Publisher> items) {
        super(items);
        this.publishers = items;
    }

    @Override
    public void clear() {
        for (Publisher publisher : publishers) {
            try {
                publisher.close();
            } catch (Exception e) {
                log.error("Error closing publisher", e);
            }
        }
        publishers = null;
    }

    @Override
    public void close() {
        clear();
    }
}
