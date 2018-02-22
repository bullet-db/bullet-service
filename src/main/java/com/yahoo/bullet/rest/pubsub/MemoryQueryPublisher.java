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
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Response;

import java.util.function.Consumer;

@Slf4j
public class MemoryQueryPublisher implements Publisher {
    MemoryPubSubConfig config;
    private AsyncHttpClient client;
    public static final int NO_TIMEOUT = -1;
    public static final int OK_200 = 200;
    String writeURI;
    String respondURI;

    public MemoryQueryPublisher(BulletConfig config) {
        this.config = new MemoryPubSubConfig(config);
        this.writeURI = getWriteURI();
        this.respondURI = getRespondURI();

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
    public void send(PubSubMessage message) throws PubSubException {
        String id = message.getId();
        log.error("------ in MemoryQueryPublisher.send() - 1");
        PubSubMessage newMessage = new PubSubMessage(id, message.getContent(), new Metadata(null, respondURI), message.getSequence());
        String json = newMessage.asJSON();
        client.preparePost(writeURI)
              .setBody(json)
              .setHeader("Content-Type", "text/plain")
              .setHeader("Accept", "application/json")
              .execute()
              .toCompletableFuture()
//            .exceptionally(this::handleException)
              .thenAcceptAsync(createResponseConsumer(id));
    }

    @Override
    public void close() {
        // Probably don't need to do anything?
    }

    private Consumer<Response> createResponseConsumer(String id) {
        // Create a closure with id
        return response -> handleResponse(id, response);
    }

    private void handleResponse(String id, Response response) {
        if (response == null || response.getStatusCode() != OK_200) {
            log.error("Failed to write message with id: {}. Couldn't reach memory pubsub server {}. Got response: {}", id, writeURI, response);
            return;
        }
        log.info("Successfully wrote message with id {}. Response was: {} {}", id, response.getStatusCode(), response.getStatusText());
    }

    private String getWriteURI() {
        String server = this.config.getAs(MemoryPubSubConfig.SERVER, String.class);
        String contextPath = this.config.getAs(MemoryPubSubConfig.CONTEXT_PATH, String.class);
        String path = PubSubController.WRITE_QUERY_PATH;
        return server + contextPath + path;
    }

    private String getRespondURI() {
        String server = this.config.getAs(MemoryPubSubConfig.SERVER, String.class);
        String contextPath = this.config.getAs(MemoryPubSubConfig.CONTEXT_PATH, String.class);
        String path = PubSubController.WRITE_RESPONSE_PATH;
        return server + contextPath + path;
    }
}
