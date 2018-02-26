/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.pubsub;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Publisher;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Response;

import java.io.IOException;
import java.util.function.Consumer;

@Slf4j
public abstract class MemoryPublisher implements Publisher {
    protected MemoryPubSubConfig config;
    protected AsyncHttpClient client;
    public static final int NO_TIMEOUT = -1;

    public MemoryPublisher(BulletConfig config) {
        this.config = new MemoryPubSubConfig(config);

        Number connectTimeout = this.config.getAs(MemoryPubSubConfig.CONNECT_TIMEOUT_MS, Number.class);
        Number retryLimit = this.config.getAs(MemoryPubSubConfig.CONNECT_RETRY_LIMIT, Number.class);
        AsyncHttpClientConfig clientConfig = new DefaultAsyncHttpClientConfig.Builder()
                .setConnectTimeout(connectTimeout.intValue())
                .setMaxRequestRetry(retryLimit.intValue())
                .setReadTimeout(NO_TIMEOUT)
                .setRequestTimeout(NO_TIMEOUT)
                .build();
        client = new DefaultAsyncHttpClient(clientConfig);
    }

    @Override
    public void send(String id, String content) throws PubSubException {
        send(new PubSubMessage(id, content, new Metadata()));
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            log.warn("Caught exception when closing AsyncHttpClient: " + e);
        }
    }

    /**
     * Send the PubSubMessage to the given uri.
     * @param uri The uri to which to send the message.
     * @param message The message to send.
     */
    protected void send(String uri, PubSubMessage message) {
        client.preparePost(uri)
              .setBody(message.asJSON())
              .setHeader("Content-Type", "text/plain")
              .setHeader("Accept", "application/json")
              .execute()
              .toCompletableFuture()
              .exceptionally(this::handleException)
              .thenAcceptAsync(createResponseConsumer(message.getId()));
    }

    /**
     * Send the PubSub message. The MemoryQueryPublisher subclass will put the response uri in the MetaData of the
     * PubSubMessage. The MemoryResponsePublisher subclass will extract the uri to determine the host to which it should
     * send the Response.
     * @param message The message to send.
     * @throws PubSubException
     */
    @Override
    public abstract void send(PubSubMessage message) throws PubSubException;

    private Consumer<Response> createResponseConsumer(String id) {
        // Create a closure with id
        return response -> handleResponse(id, response);
    }

    private void handleResponse(String id, Response response) {
        if (response == null || response.getStatusCode() != HttpStatus.SC_OK) {
            log.error("Failed to write message with id: {}. Couldn't reach memory pubsub server. Got response: {}", id, response);
            return;
        }
        log.info("Successfully wrote message with id {}. Response was: {} {}", id, response.getStatusCode(), response.getStatusText());
    }

    private Response handleException(Throwable throwable) {
        log.error("Received error while posting query", throwable);
        return null;
    }
}
