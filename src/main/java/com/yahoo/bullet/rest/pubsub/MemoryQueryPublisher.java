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
import com.yahoo.bullet.rest.pubsub.endpoints.PubSubController;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MemoryQueryPublisher extends MemoryPublisher {
    String writeURI;
    String respondURI;

    public MemoryQueryPublisher(BulletConfig config) {
        super(config);
        this.writeURI = getHostPath() + PubSubController.WRITE_QUERY_PATH;
        this.respondURI = getHostPath() + PubSubController.WRITE_RESPONSE_PATH;
    }

    @Override
    public void send(PubSubMessage message) throws PubSubException {
        // Put responseURI in the metadata so the ResponsePublisher knows to which host to send the response
        Metadata metadata = new Metadata(null, respondURI);
        PubSubMessage newMessage = new PubSubMessage(message.getId(), message.getContent(), metadata, message.getSequence());
        send(writeURI, newMessage);
    }

    private String getHostPath() {
        String server = this.config.getAs(MemoryPubSubConfig.WRITE_SERVER, String.class);
        String contextPath = this.config.getAs(MemoryPubSubConfig.CONTEXT_PATH, String.class);
        return server + contextPath;
    }
}
