/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.pubsub.endpoints;

import org.apache.http.HttpStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.http.HttpServletResponse;
import java.util.concurrent.CompletableFuture;

@Slf4j
@RestController
@ConditionalOnExpression("${bullet.pubsub.memory.pubsub.enabled:false}")
public class PubSubController {
    public static final String READ_RESPONSE_PATH = "/pubsub/read/response";
    public static final String READ_QUERY_PATH = "/pubsub/read/query";
    public static final String WRITE_RESPONSE_PATH = "/pubsub/write/response";
    public static final String WRITE_QUERY_PATH = "/pubsub/write/query";

    @Autowired
    private PubSubService pubSubService;

    @GetMapping(path = { READ_RESPONSE_PATH }, produces = { MediaType.APPLICATION_JSON_VALUE })
    public CompletableFuture<String> readResponse(HttpServletResponse response) {
        String value = pubSubService.readResponse();
        if (value == null) {
            response.setStatus(HttpStatus.SC_NO_CONTENT);
        }
        CompletableFuture<String> result = new CompletableFuture<>();
        result.complete(value);
        return result;
    }

    @GetMapping(path = { READ_QUERY_PATH }, produces = { MediaType.APPLICATION_JSON_VALUE })
    public CompletableFuture<String> readQuery(HttpServletResponse response) {
        String query = pubSubService.readQuery();
        if (query == null) {
            response.setStatus(HttpStatus.SC_NO_CONTENT);
        }
        CompletableFuture<String> result = new CompletableFuture<>();
        result.complete(query);
        return result;
    }

    @PostMapping(path = { WRITE_RESPONSE_PATH }, consumes = { MediaType.TEXT_PLAIN_VALUE }, produces = { MediaType.APPLICATION_JSON_VALUE })
    public void writeResponse(@RequestBody String response) {
        pubSubService.writeResponse(response);
    }

    @PostMapping(path = { WRITE_QUERY_PATH }, consumes = { MediaType.TEXT_PLAIN_VALUE }, produces = { MediaType.APPLICATION_JSON_VALUE })
    public void writeQuery(@RequestBody String query) {
        pubSubService.writeQuery(query);
    }
}
