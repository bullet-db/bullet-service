/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.rest.resource.QueryError;
import com.yahoo.bullet.rest.service.PubSubService;
import com.yahoo.bullet.rest.utils.HTTPQueryHandler;
import com.yahoo.bullet.rest.utils.QueryHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import java.util.UUID;

@RestController
@Path("/query")
@Produces(MediaType.APPLICATION_JSON)
public class QueryController {
    @Autowired
    private PubSubService pubSubService;

    /**
     * The method that handles POSTs to this endpoint. Consumes the HTTP request, invokes {@link PubSubService} to
     * register and transmit the query to Bullet.
     *
     * @param query The JSON query.
     * @param asyncResponse The {@link AsyncResponse} object to respond to.
     */
    @POST
    public void submitQuery(String query, @Suspended AsyncResponse asyncResponse) {
        QueryHandler queryHandler = HTTPQueryHandler.of(asyncResponse);
        if (query == null) {
            queryHandler.fail(QueryError.INVALID_QUERY);
            return;
        }
        String queryID = UUID.randomUUID().toString();
        pubSubService.submit(queryID, query, queryHandler);
    }
}
