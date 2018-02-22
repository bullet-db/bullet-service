/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.pubsub;
import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.pubsub.BufferingSubscriber;
import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MemorySubscriber extends BufferingSubscriber {
    MemoryPubSubConfig config;
    String uri;
    HttpClient client;

    public MemorySubscriber(BulletConfig config, PubSub.Context context, int maxUncommittedMessages) {
        super(maxUncommittedMessages);
        this.config = new MemoryPubSubConfig(config);

        this.uri = getURI(context);
        this.client = HttpClients.createDefault();
    }

    @Override
    public List<PubSubMessage> getMessages() throws PubSubException {
        try {
            HttpPost post = new HttpPost(uri);
            post.setHeader("Accept", "application/json");
            post.setHeader("Content-type", "text/plain");
            // This can throw, otherwise I'd put it in constructor
            post.setEntity(new StringEntity("{}")); // Empty for now - this will eventually be some info about which web service this is?

            HttpResponse response = client.execute(post);
            int statusCode = response.getStatusLine().getStatusCode();

            if (statusCode == HttpStatus.SC_NO_CONTENT) {
                // SC_NO_CONTENT status (204) indicates there are no new messages
                return null;
            }
            if (statusCode != HttpStatus.SC_OK) {
                // Can't throw error here because often times the first few calls return error codes until the service comes up
                log.error("---- writeURI: " + this.uri);
                log.error("Http call failed with status code {} and response {}.", statusCode, response);
            }
            return Collections.singletonList(PubSubMessage.fromJSON(readResponseContent(response)));
        } catch (Exception e) {
            // Can't throw error here because often times the first few calls return error codes until the service comes up
            log.error("Http post failed with error: " + e);
            //throw new PubSubException("Http post failed with error: " + e);
        }
        return null;
    }

    protected String readResponseContent(HttpResponse response) throws UnsupportedOperationException, IOException {
        BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), "UTF-8"));
        StringBuffer result = new StringBuffer();
        String line = null;
        while ((line = rd.readLine()) != null) {
            result.append(line);
            result.append('\n'); // try to comment this out and see what happens
        }
        return result.toString();
    }

    private String getURI(PubSub.Context context) {
        String server = this.config.getAs(MemoryPubSubConfig.WRITE_SERVER, String.class);
        String path = context == PubSub.Context.QUERY_PROCESSING ?
                      this.config.getAs(MemoryPubSubConfig.READ_QUERY_PATH, String.class) :
                      this.config.getAs(MemoryPubSubConfig.READ_RESPONSE_PATH, String.class);
        return server + path;
    }

    @Override
    public void close() {
        // Probably do nothing?
    }
}
