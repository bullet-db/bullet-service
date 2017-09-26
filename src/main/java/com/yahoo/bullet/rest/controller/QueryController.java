/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.rest.query.HTTPQueryHandler;
import com.yahoo.bullet.rest.query.QueryError;
import com.yahoo.bullet.rest.query.QueryHandler;
import com.yahoo.bullet.rest.service.QueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import java.util.UUID;

@RestController
public class QueryController {
    @Autowired
    private QueryService queryService;

    /**
     * The method that handles POSTs to this endpoint. Consumes the HTTP request, invokes {@link QueryService} to
     * register and transmit the query to Bullet.
     *
     * @param query The JSON query.
     * @param asyncResponse The {@link AsyncResponse} object to respond to.
     */
    @RequestMapping(path = "/query", consumes = { MediaType.APPLICATION_JSON_VALUE },
                    produces = { MediaType.APPLICATION_JSON_VALUE })
    public void submitQuery(String query, @Suspended AsyncResponse asyncResponse) {
        QueryHandler queryHandler = HTTPQueryHandler.of(asyncResponse);
        if (query == null) {
            queryHandler.fail(QueryError.INVALID_QUERY);
            return;
        }
        String queryID = UUID.randomUUID().toString();
        queryService.submit(queryID, query, queryHandler);
    }
}
