/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.pubsub.controller;

import com.yahoo.bullet.rest.pubsub.service.PubSubService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
@RestController
public class PubSubController {
    @Autowired
    private PubSubService pubSubService;

    @PostMapping(path = "/pubsub/read/response", consumes = { MediaType.TEXT_PLAIN_VALUE }, produces = { MediaType.APPLICATION_JSON_VALUE })
    public CompletableFuture<String> readResponse(@RequestBody String input) {
        String response = pubSubService.readResponse(input);
        CompletableFuture<String> result = new CompletableFuture<>();
        result.complete(response);
        return result;
    }

    @PostMapping(path = "/pubsub/read/query", consumes = { MediaType.TEXT_PLAIN_VALUE }, produces = { MediaType.APPLICATION_JSON_VALUE })
    public CompletableFuture<String> readQuery(@RequestBody String input) {
        String query = pubSubService.readQuery(input);
        CompletableFuture<String> result = new CompletableFuture<>();
        result.complete(query);
        return result;
    }

    @PostMapping(path = "/pubsub/publish/response", consumes = { MediaType.TEXT_PLAIN_VALUE }, produces = { MediaType.APPLICATION_JSON_VALUE })
    public CompletableFuture<String> publishResponse(@RequestBody String response) {
        Integer returnValue = pubSubService.writeResponse(response);
        CompletableFuture<String> result = new CompletableFuture<>();
        result.complete(returnValue.toString());
        return result;
    }

    @PostMapping(path = "/pubsub/publish/query", consumes = { MediaType.TEXT_PLAIN_VALUE }, produces = { MediaType.APPLICATION_JSON_VALUE })
    public CompletableFuture<String> publishQuery(@RequestBody String query) {
        Integer test = pubSubService.writeQuery(query);
        //Integer returnValue = pubSubService.writeQuery(query);
        // HTTPQueryHandler queryHandler = new HTTPQueryHandler();
//        if (query == null) {
//            queryHandler.fail(QueryError.INVALID_QUERY);
//        } else {
//            String queryID = UUID.randomUUID().toString();
//            queryService.submit(queryID, query, queryHandler);
//        }

        CompletableFuture<String> result = new CompletableFuture<>();
        result.complete("This is line 43 of QueryWriterController. writeQuery response: " + test.toString());
        return result;
    }
}