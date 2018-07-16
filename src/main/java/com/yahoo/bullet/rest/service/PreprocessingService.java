/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.yahoo.bullet.bql.BulletQueryBuilder;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.rest.query.QueryError;
import com.yahoo.bullet.rest.query.QueryHandler;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class PreprocessingService {
    private static final String WINDOW_KEY_STRING = "window";
    private static final BulletQueryBuilder QUERY_BUILDER = new BulletQueryBuilder(new BulletConfig());
    private static final Gson GSON = new GsonBuilder().create();

    /**
     * Convert this query to a valid JSON Bullet Query if it is currently a BQL query.
     *
     * @param query The query to convert.
     * @param queryHandler The {@link QueryHandler} object that handles the query.
     * @return The valid JSON Bullet query.
     */
    public String convertIfBQL(String query, QueryHandler queryHandler) {
        try {
            query = convertIfBQL(query);
        } catch (Exception e) {
            queryHandler.fail(QueryError.INVALID_QUERY);
            return null;
        }
        return query;
    }

    /**
     * Fail the query if the query contains a window.
     *
     * @param query The query to check.
     * @param queryHandler The {@link QueryHandler} object that handles the query.
     */
    @SuppressWarnings("unchecked")
    public void failIfWindowed(String query, QueryHandler queryHandler) {
        query = convertIfBQL(query, queryHandler);
        if (queryHandler.isComplete()) {
            return;
        }
        try {
            Map<String, Object> queryContent = GSON.fromJson(query, Map.class);
            if (queryContent.containsKey(WINDOW_KEY_STRING) && queryContent.get(WINDOW_KEY_STRING) != null) {
                queryHandler.fail(QueryError.UNSUPPORTED_QUERY);
            }
        } catch (Exception e) {
            queryHandler.fail(QueryError.INVALID_QUERY);
        }
    }

    private static String convertIfBQL(String query) throws Exception {
        if (query == null || query.trim().charAt(0) == '{') {
            return query;
        } else {
            return QUERY_BUILDER.buildJson(query);
        }
    }
}
