/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

package com.yahoo.bullet.rest.query;

import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Subscriber;
import com.yahoo.bullet.rest.query.QueryHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class PubSubReader {
    @Getter
    private static AtomicInteger activeReaderCount = new AtomicInteger(0);
    private Subscriber subscriber;
    private ConcurrentMap<String, QueryHandler> requestQueue;
    private Thread readerThread;
    private int sleepTimeMS;

    /**
     * Create a service with a {@link Subscriber} and a request queue.
     *
     * @param subscriber The Subscriber to read responses from.
     * @param requestQueue The {@link ConcurrentMap} containing open requests.
     * @param sleepTimeMS The duration to sleep for if PubSub receive is empty. Helps prevent busy waiting.
     */
    public PubSubReader(Subscriber subscriber, ConcurrentMap<String, QueryHandler> requestQueue, int sleepTimeMS) {
        this.subscriber = subscriber;
        this.requestQueue = requestQueue;
        this.sleepTimeMS = sleepTimeMS;
        this.readerThread = new Thread(this::run);
        readerThread.start();
    }

    /**
     * Interrupt the reader thread and close the {@link Subscriber}.
     */
    public void close() {
        readerThread.interrupt();
    }

    /**
     * Read responses from the Pub/Sub and update requests.
     */
    public void run() {
        PubSubMessage response;
        activeReaderCount.incrementAndGet();
        log.info("Reader thread started, ID: " + Thread.currentThread().getId());
        while (!Thread.currentThread().isInterrupted()) {
            try {
                response = subscriber.receive();
                if (response == null) {
                    Thread.sleep(sleepTimeMS);
                    continue;
                }
                QueryHandler queryHandler = requestQueue.get(response.getId());
                if (queryHandler == null) {
                    subscriber.commit(response.getId());
                    continue;
                }
                synchronized (queryHandler) {
                    if (queryHandler.isComplete()) {
                        continue;
                    }
                    queryHandler.send(response);
                    // TODO: Do this when a Signal.COMPLETE is received.
                    // TODO: Handle Signal.ACKNOWLEDGE messages.
                    queryHandler.complete();
                    requestQueue.remove(response.getId());
                }
                subscriber.commit(response.getId());
            } catch (Exception e) {
                // When the reader is closed, this block also catches InterruptedException's from Thread.sleep.
                // If the service is busy reading messages, the while loop will break instead.
                log.error("Closing reader thread with error: " + e);
                break;
            }
        }
        subscriber.close();
        activeReaderCount.decrementAndGet();
    }
}
