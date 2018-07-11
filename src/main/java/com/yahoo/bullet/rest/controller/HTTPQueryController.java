/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.yahoo.bullet.bql.BulletQueryBuilder;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.rest.query.HTTPQueryHandler;
import com.yahoo.bullet.rest.query.QueryError;
import com.yahoo.bullet.rest.query.SSEQueryHandler;
import com.yahoo.bullet.rest.service.QueryService;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RestController
public class HTTPQueryController {
    private static final String WINDOW_KEY_STRING = "window";
    @Autowired @Setter
    private QueryService queryService;
    private static final Gson GSON = new GsonBuilder().create();
    private static final BulletQueryBuilder bulletQueryBuilder = new BulletQueryBuilder(new BulletConfig());
    private static final JsonParser jsonParser = new JsonParser();

    /**
     * The method that handles POSTs to this endpoint. Consumes the HTTP request, invokes {@link QueryService} to
     * register and transmit the query to Bullet.
     *
     * @param query The JSON query.
     * @return A {@link CompletableFuture} representing the eventual result.
     */
    @PostMapping(path = "${bullet.endpoint.http}", consumes = { MediaType.TEXT_PLAIN_VALUE }, produces = { MediaType.APPLICATION_JSON_VALUE })
    @SuppressWarnings("unchecked")
    public CompletableFuture<String> submitHTTPQuery(@RequestBody String query) {
        HTTPQueryHandler queryHandler = new HTTPQueryHandler();
        String queryID = QueryService.getNewQueryID();
        try {
            query = convertNonJSONToBQL(query);
            Map<String, Object> queryContent = GSON.fromJson(query, Map.class);
            if (queryContent.containsKey(WINDOW_KEY_STRING) && queryContent.get(WINDOW_KEY_STRING) != null) {
                queryHandler.fail(QueryError.UNSUPPORTED_QUERY);
            } else {
                queryService.submit(queryID, query, queryHandler);
            }
        } catch (Exception e) {
            queryHandler.fail(QueryError.INVALID_QUERY);
        }
        return queryHandler.getResult();
    }

    /**
     * The method that handles SSE POSTs to this endpoint. Consumes the HTTP request, invokes {@link QueryService} to
     * register and transmit the query to Bullet.
     *
     * @param query The JSON query.
     * @return A {@link SseEmitter} to send streaming results.
     */
    @PostMapping(value = "${bullet.endpoint.sse}", consumes = { MediaType.TEXT_PLAIN_VALUE }, produces = { MediaType.APPLICATION_JSON_VALUE })
    public SseEmitter submitSSEQuery(@RequestBody String query) {
        SseEmitter sseEmitter = new SseEmitter();
        String queryID = QueryService.getNewQueryID();
        SSEQueryHandler sseQueryHandler = new SSEQueryHandler(queryID, sseEmitter, queryService);
        try {
            query = convertNonJSONToBQL(query);
        } catch (Exception e) {
            sseQueryHandler.fail(QueryError.INVALID_QUERY);
            return sseEmitter;
        }
        // query should not be null at this point. If the post body is null, Springframework will return 400 directly.
        queryService.submit(queryID, query, sseQueryHandler);
        return sseEmitter;
    }

    private String convertNonJSONToBQL(String query) throws Exception {
        try {
            jsonParser.parse(query);
        } catch (JsonParseException e) {
            return bulletQueryBuilder.buildJson(query);
        }
        return query;
    }
}
