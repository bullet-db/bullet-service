/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.pubsub;

import com.yahoo.bullet.pubsub.PubSubMessage;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class PubSubService {
    private List<PubSubMessage> queryList = new ArrayList<>();
    private List<PubSubMessage> responseList = new ArrayList<>();

    public Integer writeResponse(String response) {
        responseList.add(PubSubMessage.fromJSON(response));
        return responseList.size();
    }

    public PubSubMessage readResponse() {
        return responseList.remove(0);
    }

    public Integer writeQuery(String query) {
        queryList.add(PubSubMessage.fromJSON(query));
        return queryList.size();
    }

    public PubSubMessage readQuery(String query) {
        return queryList.remove(0);
    }

}
