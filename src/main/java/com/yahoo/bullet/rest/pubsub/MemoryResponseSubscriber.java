/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.pubsub;

import com.yahoo.bullet.pubsub.BufferingSubscriber;
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
public class MemoryResponseSubscriber extends BufferingSubscriber {

    public MemoryResponseSubscriber() {
        super(100); // setting buffer size - FIX THIS
    }

    @Override
    public List<PubSubMessage> getMessages() throws PubSubException {
        String url = "http://localhost:9999/api/bullet/pubsub/read/response";

        try {
            HttpClient httpClient = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost(url);
            httpPost.setHeader("Accept", "application/json");
            httpPost.setHeader("Content-type", "text/plain");
            httpPost.setEntity(new StringEntity("{}")); // Empty for now - this will eventually be some info about which web service this is?

            HttpResponse response = httpClient.execute(httpPost);

            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                log.error("STATUS CODE NOT OK!!! response: " + response);
            }

            String jsonContent = readResponseContent(response);
            if (jsonContent == null) {
                //log.error("In MemoryResponseSubscriber jsonContent is null (not a string) - jsonContent: " + jsonContent);
                return null;
            } else if (jsonContent.equalsIgnoreCase("null\n")) {
                //log.error("In MemoryResponseSubscriber jsonContent is a String that reads null - jsonContent: " + jsonContent);
                return null;
            } else {
                //log.error("In MemoryResponseSubscriber - returning a singleton list 88 88 - jsonContent = " + jsonContent + ".  jsonContent.length = " + jsonContent.length());
                return Collections.singletonList(PubSubMessage.fromJSON(jsonContent));
            }
        } catch (Exception e) {
            log.error("EXCEPTION CAUGHT!: " + e);
        }
        return null;
    }

    protected String readResponseContent(HttpResponse response) throws UnsupportedOperationException, IOException {
        BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), "UTF-8"));

        StringBuffer result = new StringBuffer();
        String line = null;
        while ((line = rd.readLine()) != null) {
            result.append(line);
            result.append('\n');
        }
        return result.toString();
    }

    @Override
    public void close() {
        // Probably do nothing?
    }

}
