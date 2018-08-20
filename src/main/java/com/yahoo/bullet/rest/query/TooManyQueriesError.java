/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.query;

public class TooManyQueriesError extends QueryError {
    /**
     * Constructor.
     *
     * @param e The Throwable indicating the reason for the error.
     */
    public TooManyQueriesError(Throwable e) {
        super(e.getCause().toString(), "Please try again later.");
    }
}
