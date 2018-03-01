/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.pubsub;

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
    @Autowired
    private PubSubService pubSubService;

    @PostMapping(path = "${bullet.pubsub.memory.pubsub.result.path}", consumes = { MediaType.TEXT_PLAIN_VALUE })
    public void writeResponse(@RequestBody String response) {
        pubSubService.writeResponse(response);
    }

    @GetMapping(path = "${bullet.pubsub.memory.pubsub.result.path}", produces = { MediaType.APPLICATION_JSON_VALUE })
    public CompletableFuture<String> readResponse(HttpServletResponse response) {
        String value = pubSubService.readResponse();
        if (value == null) {
            response.setStatus(HttpStatus.SC_NO_CONTENT);
        }
        CompletableFuture<String> result = new CompletableFuture<>();
        result.complete(value);
        return result;
    }

    @PostMapping(path = "${bullet.pubsub.memory.pubsub.query.path}", consumes = { MediaType.TEXT_PLAIN_VALUE })
    public void writeQuery(@RequestBody String query) {
        pubSubService.writeQuery(query);
    }

    @GetMapping(path = "${bullet.pubsub.memory.pubsub.query.path}", produces = { MediaType.APPLICATION_JSON_VALUE })
    public CompletableFuture<String> readQuery(HttpServletResponse response) {
        String query = pubSubService.readQuery();
        if (query == null) {
            response.setStatus(HttpStatus.SC_NO_CONTENT);
        }
        CompletableFuture<String> result = new CompletableFuture<>();
        result.complete(query);
        return result;
    }
}
