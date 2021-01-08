/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

package com.yahoo.bullet.rest.common;

import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.PubSubResponder;
import com.yahoo.bullet.pubsub.Subscriber;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

@Slf4j
public class Reader {
    private Subscriber subscriber;
    private PubSubResponder responder;
    private Thread reader;
    private int sleepTimeMS;
    /**
     * Create a service with a {@link Subscriber} and a request queue.
     *
     * @param subscriber The Subscriber to read responses from.
     * @param responder The {@link PubSubResponder} to use to respond.
     * @param sleepTimeMS The duration to sleep for if PubSub receive is empty. Helps prevent busy waiting.
     */
    public Reader(Subscriber subscriber, PubSubResponder responder, int sleepTimeMS) {
        Objects.requireNonNull(subscriber);
        Objects.requireNonNull(responder);
        this.subscriber = subscriber;
        this.responder = responder;
        this.sleepTimeMS = sleepTimeMS;
        this.reader = new Thread(this::run);
    }

    /**
     * Starts reading from the pubsub.
     */
    public void start() {
        reader.start();
    }

    /**
     * Interrupt the reader thread and close the {@link Subscriber}.
     */
    public void close() {
        reader.interrupt();
    }

    /**
     * Read responses from the PubSub and update requests.
     */
    public void run() {
        PubSubMessage message;
        log.info("Reader thread started, ID: {}", Thread.currentThread().getId());
        while (!Thread.interrupted()) {
            try {
                message = subscriber.receive();
                if (message == null) {
                    Thread.sleep(sleepTimeMS);
                    continue;
                }
                log.debug("Received message {}", message);
                responder.respond(message.getId(), message);
                subscriber.commit(message.getId());
            } catch (InterruptedException | PubSubException e) {
                // When the reader is closed, this block also catches InterruptedException from Thread.sleep.
                // If the service is busy reading messages, the while loop will break instead.
                log.error("Closing reader thread with error", e);
                break;
            } catch (Exception e) {
                log.error("Unable to fully process and/or respond to message! Continuing...", e);
            }
        }
        try {
            subscriber.close();
        } catch (Exception e) {
            log.error("Error closing subscriber", e);
        }
    }
}
