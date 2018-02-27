/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.query;

import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta;
import com.yahoo.bullet.parsing.ParsingError;
import lombok.Getter;

@Getter
public class QueryError {
    public static final QueryError INVALID_QUERY = new QueryError("Query not found.", "Please provide a valid query.");
    public static final QueryError SERVICE_UNAVAILABLE = new QueryError("Service temporarily unavailable", "Please try again later.");

    private String error;
    private String resolution;

    /**
     * Constructor that takes an error message and resolution for it.
     *
     * @param error The error message.
     * @param resolution The resolution that can be taken.
     */
    public QueryError(String error, String resolution) {
        this.error = error;
        this.resolution = resolution;
    }

    @Override
    public String toString() {
        return Clip.of(Meta.of(ParsingError.makeError(error, resolution))).asJSON();
    }
}
