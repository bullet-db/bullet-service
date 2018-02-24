/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.pubsub;

import com.yahoo.bullet.BulletConfig;
import lombok.extern.slf4j.Slf4j;
import java.util.Collections;
import java.util.List;

@Slf4j
public class MemoryResponseSubscriber extends MemorySubscriber {

    public MemoryResponseSubscriber(BulletConfig config, int maxUncommittedMessages) {
        super(config, maxUncommittedMessages);
    }

    @Override
    protected List<String> getURIs() {
        String path = this.config.getAs(MemoryPubSubConfig.READ_RESPONSE_PATH, String.class);
        return Collections.singletonList(this.config.getAs(MemoryPubSubConfig.WRITE_SERVER, String.class) + path);
    }
}
