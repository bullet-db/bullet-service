/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.yahoo.bullet.bql.BulletQueryBuilder;
import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.rest.query.BQLException;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Value;

import java.util.Map;

@Service
public class PreprocessingService {
    private static final String WINDOW_KEY_STRING = "window";
    private static final BulletQueryBuilder QUERY_BUILDER = new BulletQueryBuilder(new BulletConfig());
    private static final Gson GSON = new GsonBuilder().create();
    private static final long QUERY_DEFAULT_DURATION_MS;
    private static final long QUERY_MAX_DURATION_MS;

    static {
        BulletConfig config = new BulletConfig();
        QUERY_DEFAULT_DURATION_MS = (long) config.get(BulletConfig.QUERY_DEFAULT_DURATION);
        QUERY_MAX_DURATION_MS = (long) config.get(BulletConfig.QUERY_MAX_DURATION);
    }

    @Value("${bullet.max.concurrent.queries}")
    private int maxConcurrentQueries;

    /**
     * Convert this query to a valid JSON Bullet Query if it is currently a BQL query.
     *
     * @param query The query to convert.
     * @return The valid JSON Bullet query.
     * @throws BQLException when there is an error with the BQL conversion.
     */
    public String convertIfBQL(String query) throws Exception {
        try {
            if (query == null || query.trim().charAt(0) == '{') {
                return query;
            } else {
                return QUERY_BUILDER.buildJson(query);
            }
        } catch (ParsingException | UnsupportedOperationException e) {
            throw new BQLException(e);
        }
    }

    /**
     *
     * @param query
     * @return
     */
    public long getTimeoutDuration(String query) {
        Query object = GSON.fromJson(query, Query.class);
        if (object == null || object.getDuration() == null || object.getDuration() <= 0L) {
            return QUERY_DEFAULT_DURATION_MS;
        }
        if (object.getDuration() > QUERY_MAX_DURATION_MS) {
            return QUERY_MAX_DURATION_MS;
        }
        return object.getDuration();
    }

    /**
     * This function checks if the configured max.concurrent.queries limit has been exceeded.
     *
     * @return A boolean indicating whether or not the query limit has been reached.
     */
    public boolean queryLimitReached(QueryService queryService) {
        return queryService.runningQueryCount() >= maxConcurrentQueries;
    }

    /**
     * This function determines if the provided query contains a window.
     *
     * @param query The query to check. The query must be in JSON format.
     * @return A boolean indicating whether or not this query contains a window.
     * @throws JsonSyntaxException if json is not a valid representation for a JSON object.
     */
    @SuppressWarnings("unchecked")
    public boolean containsWindow(String query) throws JsonSyntaxException {
        Map<String, Object> queryContent = GSON.fromJson(query, Map.class);
        return queryContent.containsKey(WINDOW_KEY_STRING) && queryContent.get(WINDOW_KEY_STRING) != null;
    }
}
