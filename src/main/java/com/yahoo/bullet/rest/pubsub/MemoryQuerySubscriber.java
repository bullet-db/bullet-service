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
public class MemoryQuerySubscriber extends BufferingSubscriber {

    public MemoryQuerySubscriber() {
        super(100);
    }

    @Override
    public List<PubSubMessage> getMessages() throws PubSubException {
        String url = "http://localhost:9999/api/bullet/pubsub/read/query";
        //log.error("----At the top of getMessages()");

        try {
            HttpClient httpClient = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost(url);
            httpPost.setHeader("Accept", "application/json");
            httpPost.setHeader("Content-type", "text/plain");
            httpPost.setEntity(new StringEntity("{}")); // Empty for now - this will eventually be some info about which node this is?
            //log.error("------ Calling execute()");
            HttpResponse response = httpClient.execute(httpPost);

            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                //log.error(" ----- STATUS CODE NOT OK!!! response: " + response);
            }

            String jsonContent = readResponseContent(response);
            if (jsonContent == null) {
                //log.error("In MemoryQuerySubscriber jsonContent is null (not a string) - jsonContent: " + jsonContent);
                return null;
            } else if (jsonContent.equalsIgnoreCase("null\n")) {
                //log.error("In MemoryResponseSubscriber jsonContent is a String that reads null - jsonContent: " + jsonContent);
                return null;
            } else {
                //log.error("In MemoryQuerySubscriber - returning a singleton list 88 88 - jsonContent = " + jsonContent + ".  jsonContent.length = " + jsonContent.length());
                return Collections.singletonList(PubSubMessage.fromJSON(jsonContent));
            }
        } catch (Exception e) {
            log.error("EXCEPTION CAUGHT!: " + e);
        }
        //log.error("---------- Now at the very bottom of getMessages()");
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

//    @Override
//    public List<PubSubMessage> getMessages() throws PubSubException {
////        ConsumerRecords<String, byte[]> buffer;
////        try {
////            buffer = consumer.poll(0);
////        } catch (KafkaException e) {
////            throw new PubSubException("Consumer poll failed", e);
////        }
////        List<PubSubMessage> messages = new ArrayList<>();
////        for (ConsumerRecord<String, byte[]> record : buffer) {
////            Object message = SerializerDeserializer.fromBytes(record.value());
////            if (message == null || !(message instanceof PubSubMessage)) {
////                log.warn("Invalid message received: {}", message);
////                continue;
////            }
////            messages.add((PubSubMessage) message);
////        }
////        if (manualCommit) {
////            consumer.commitAsync();
////        }
//
//
//        // Do http request here
//        List<PubSubMessage> messages = new ArrayList<>();
//        return messages;
//    }

    @Override
    public void close() {
        // Probably do nothing?
    }

}
