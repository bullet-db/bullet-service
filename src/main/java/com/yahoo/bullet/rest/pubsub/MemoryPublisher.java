/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.pubsub;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSub;
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
public class MemoryPublisher implements Publisher {
    MemoryPubSubConfig config;
    private AsyncHttpClient client;
    public static final int NO_TIMEOUT = -1;
    public static final int OK_200 = 200;
    String uri;

    //public MemoryPublisher(BulletConfig config, String url) {
    public MemoryPublisher(BulletConfig config, PubSub.Context context) {
        this.config = new MemoryPubSubConfig(config);

        String server = this.config.getAs(MemoryPubSubConfig.SERVER, String.class);
        String servletContext = this.config.getAs(MemoryPubSubConfig.SERVLET_CONTEXT, String.class);
        String path = context == PubSub.Context.QUERY_PROCESSING ?
                      PubSubController.WRITE_RESPONSE_PATH : PubSubController.WRITE_QUERY_PATH;
        this.uri = server + servletContext + path;

        // Get these from the config!!
        Number connectTimeout = 30000;
        Number retryLimit = 10;
        AsyncHttpClientConfig clientConfig = new DefaultAsyncHttpClientConfig.Builder()
                .setConnectTimeout(connectTimeout.intValue())
                .setMaxRequestRetry(retryLimit.intValue())
                .setReadTimeout(NO_TIMEOUT)
                .setRequestTimeout(NO_TIMEOUT)
                .build();
        // This is thread safe
        client = new DefaultAsyncHttpClient(clientConfig);
    }

    @Override
    public void send(String id, String content) throws PubSubException {
        send(new PubSubMessage(id, content, new Metadata()));
    }

    @Override
    public void send(PubSubMessage message) throws PubSubException {
        log.error("------ NEW artifact (feb 19) with uri: " + uri);
        //String url = urls.get();
        String id = message.getId();
        String json = message.asJSON();
        client.preparePost(uri)
                .setBody(json)
                .setHeader("Content-Type", "text/plain")
                .setHeader("Accept", "application/json")
                .execute()
                .toCompletableFuture()
//              .exceptionally(this::handleException)
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
            // Deal with error
            log.error("----------Error! Response: " + response);
            //log.error("Handling error for id {} with response {}", id, response);
            //responses.offer(new PubSubMessage(id, DRPCError.CANNOT_REACH_DRPC.asJSON()));
            return;
        }
        log.error("----------Seems good, just want to see what this is: Response: " + response);
//        log.info("Received for id {}: {} {}", response.getStatusCode(), id, response.getStatusText());
//        String body = response.getResponseBody();
//        PubSubMessage message = PubSubMessage.fromJSON(body);
//        log.debug("Received for id {}:\n{}", message.getId(), message.getContent());
//        responses.offer(message);
    }

}
