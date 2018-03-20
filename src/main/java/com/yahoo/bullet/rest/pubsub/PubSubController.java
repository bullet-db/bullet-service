/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.pubsub;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import com.yahoo.bullet.pubsub.rest.RESTPubSub;

import javax.servlet.http.HttpServletResponse;

@RestController
@ConditionalOnExpression("${bullet.pubsub.builtin.rest.enabled:false}")
public class PubSubController {
    @Autowired
    private PubSubService pubSubService;

    /**
     * The method that handles adding results to the result queue. Clients should POST to this endpoint to write results
     * to the queue. Invokes {@link PubSubService} to add the result to the queue.
     *
     * @param result The result to add to the queue.
     */
    @PostMapping(path = "${bullet.pubsub.builtin.rest.result.path}", consumes = { MediaType.APPLICATION_JSON_VALUE })
    public void postResult(@RequestBody String result) {
        pubSubService.postResult(result);
    }

    /**
     * The method that handles reading a result. Clients should GET from this endpoint to read a result. Returns
     * NO_CONTENT (204) if there are no results to read.
     *
     * @param response The {@link HttpServletResponse} that will be used to set the response status code.
     * @return A String representing the result.
     */
    @GetMapping(path = "${bullet.pubsub.builtin.rest.result.path}", produces = { MediaType.APPLICATION_JSON_VALUE })
    public String getResult(HttpServletResponse response) {
        String result = pubSubService.getResult();
        if (result == null) {
            response.setStatus(RESTPubSub.NO_CONTENT_204);
        }
        return result;
    }

    /**
     * The method that handles adding queries to the query queue. Clients should POST to this endpoint to write queries
     * to the queue. Invokes {@link PubSubService} to add the query to the queue.
     *
     * @param query The query to add to the queue.
     */
    @PostMapping(path = "${bullet.pubsub.builtin.rest.query.path}", consumes = { MediaType.APPLICATION_JSON_VALUE })
    public void postQuery(@RequestBody String query) {
        pubSubService.postQuery(query);
    }

    /**
     * The method that handles reading a query. Clients should GET from this endpoint to read a query. Returns
     * NO_CONTENT (204) if there are no queries to read.
     *
     * @param response The {@link HttpServletResponse} that will be used to set the response status code.
     * @return A String representing the query.
     */
    @GetMapping(path = "${bullet.pubsub.builtin.rest.query.path}", produces = { MediaType.APPLICATION_JSON_VALUE })
    public String getQuery(HttpServletResponse response) {
        String query = pubSubService.getQuery();
        if (query == null) {
            response.setStatus(RESTPubSub.NO_CONTENT_204);
        }
        return query;
    }
}
