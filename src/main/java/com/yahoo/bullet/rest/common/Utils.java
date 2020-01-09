/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.common;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class Utils {
    private static final Set<Metadata.Signal> FINISHED =
        new HashSet<>(Arrays.asList(Metadata.Signal.KILL, Metadata.Signal.COMPLETE, Metadata.Signal.FAIL));

    /**
     * Get a new unique query ID.
     *
     * @return A new unique query ID.
     */
    public static String getNewQueryID() {
        return UUID.randomUUID().toString();
    }

    /**
     * Check if this message signifies if a query is done or finished.
     *
     * @param message The {@link PubSubMessage} to check.
     * @return A boolean denoting if this message signifies that the query it is about is done.
     */
    public static boolean isDone(PubSubMessage message) {
        return message.hasSignal() && FINISHED.contains(message.getMetadata().getSignal());
    }

    /**
     * Check and throw an exception if the given collection is empty.
     *
     * @param items The {@link Collection} to check.
     * @param <T> The type of the item in the collection.
     * @throws UnsupportedOperationException if the given collection was empty.
     */
    public static <T> void checkNotEmpty(Collection<T> items) {
        if (items == null || items.isEmpty()) {
            throw new UnsupportedOperationException("Must be provided");
        }
    }
}
