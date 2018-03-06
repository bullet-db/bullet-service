/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.pubsub;

import org.springframework.http.HttpStatus;
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
    public void postResult(@RequestBody String response) {
        pubSubService.postResult(response);
    }

    @GetMapping(path = "${bullet.pubsub.memory.pubsub.result.path}", produces = { MediaType.APPLICATION_JSON_VALUE })
    public CompletableFuture<String> getResult(HttpServletResponse response) {
        String value = pubSubService.getResult();
        if (value == null) {
            response.setStatus(HttpStatus.NO_CONTENT.value());
        }
        CompletableFuture<String> result = new CompletableFuture<>();
        result.complete(value);
        return result;
    }

    @PostMapping(path = "${bullet.pubsub.memory.pubsub.query.path}", consumes = { MediaType.TEXT_PLAIN_VALUE })
    public void postQuery(@RequestBody String query) {
        pubSubService.postQuery(query);
    }

    @GetMapping(path = "${bullet.pubsub.memory.pubsub.query.path}", produces = { MediaType.APPLICATION_JSON_VALUE })
    public CompletableFuture<String> getQuery(HttpServletResponse response) {
        String query = pubSubService.getQuery();
        if (query == null) {
            response.setStatus(HttpStatus.NO_CONTENT.value());
        }
        CompletableFuture<String> result = new CompletableFuture<>();
        result.complete(query);
        return result;
    }
}
