/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.query;

public class TooManyQueriesException extends Exception {
    /**
     * Constructs a new exception with the specified message.
     *
     * @param message the message.
     */
    public TooManyQueriesException(String message) {
        super(message);
    }
}
