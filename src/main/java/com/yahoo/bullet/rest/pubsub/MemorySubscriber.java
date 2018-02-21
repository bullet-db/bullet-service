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
    //HttpClient client;

    public MemorySubscriber(BulletConfig config, PubSub.Context context, int maxUncommittedMessages) {
        super(maxUncommittedMessages);
        this.config = new MemoryPubSubConfig(config);

        this.uri = getURI(context);
        //this.uri = "http://localhost:9999/api/bullet/pubsub/read/response";
    }

    @Override
    public List<PubSubMessage> getMessages() throws PubSubException {
        log.error("in MemorySubscriber getMessages() - 0");
        try {
            log.error("in MemorySubscriber getMessages() - 1");
            HttpClient client = HttpClients.createDefault();
            HttpPost post = new HttpPost(uri);
            post.setHeader("Accept", "application/json");
            post.setHeader("Content-type", "text/plain");
            // This can throw, otherwise I'd put it in constructor
            post.setEntity(new StringEntity("{}")); // Empty for now - this will eventually be some info about which web service this is?

            log.error("in MemorySubscriber getMessages() - 2");
            HttpResponse response = client.execute(post);
            int statusCode = response.getStatusLine().getStatusCode();

            log.error("in MemorySubscriber getMessages() - 3");
            if (statusCode == HttpStatus.SC_NO_CONTENT) {
                // Status 204 indicates there are no new messages
                log.error("in MemorySubscriber getMessages() - 4");
                return null;
            }
            if (statusCode != HttpStatus.SC_OK) {
                log.error("in MemorySubscriber getMessages() - 5");
                // Can't throw error here because often times the first few calls return error codes until the service comes up
                log.error("Http call failed with status code {} and response {}.", statusCode, response);
            }
            return Collections.singletonList(PubSubMessage.fromJSON(readResponseContent(response)));
        } catch (Exception e) {
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
        String server = this.config.getAs(MemoryPubSubConfig.SERVER, String.class);
        String contextPath = this.config.getAs(MemoryPubSubConfig.CONTEXT_PATH, String.class);
        String path = context == PubSub.Context.QUERY_PROCESSING ?
                      PubSubController.READ_QUERY_PATH : PubSubController.READ_RESPONSE_PATH;
        return server + contextPath + path;
    }

    @Override
    public void close() {
        // Probably do nothing?
    }
}
