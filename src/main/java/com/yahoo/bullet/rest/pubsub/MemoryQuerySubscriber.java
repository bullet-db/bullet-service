/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.pubsub;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.rest.pubsub.endpoints.PubSubController;
import lombok.extern.slf4j.Slf4j;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class MemoryQuerySubscriber extends MemorySubscriber {

    public MemoryQuerySubscriber(BulletConfig config, int maxUncommittedMessages) {
        super(config, maxUncommittedMessages);
    }

    @Override
    protected List<String> getURIs() {
        String[] servers = this.config.getAs(MemoryPubSubConfig.READ_SERVERS, String.class).split(",");
        String contextPath = this.config.getAs(MemoryPubSubConfig.CONTEXT_PATH, String.class);
        String path = PubSubController.READ_QUERY_PATH;
        return Arrays.asList(servers).stream().map(s -> s + contextPath + path).collect(Collectors.toList());
    }
}
