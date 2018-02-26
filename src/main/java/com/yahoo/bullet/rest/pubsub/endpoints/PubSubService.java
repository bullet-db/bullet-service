/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.pubsub.endpoints;

import com.yahoo.bullet.pubsub.PubSubMessage;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class PubSubService {
    private List<PubSubMessage> queryList = new ArrayList<>();
    private List<PubSubMessage> responseList = new ArrayList<>();

    public String readQuery() {
        return queryList.isEmpty() ? "null" : queryList.remove(0).asJSON();
    }

    public String readResponse() {
        return responseList.isEmpty() ? "null" : responseList.remove(0).asJSON();
    }

    public Integer writeResponse(String response) {
        responseList.add(PubSubMessage.fromJSON(response));
        return responseList.size();
    }

    public Integer writeQuery(String query) {
        queryList.add(PubSubMessage.fromJSON(query));
        return queryList.size();
    }
}
