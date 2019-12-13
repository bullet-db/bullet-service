/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.common;

public class BQLException extends Exception {
    private static final long serialVersionUID = -5247117284749963789L;

    /**
     * Constructs a new exception with the specified cause.
     *
     * @param cause the cause.
     */
    public BQLException(Throwable cause) {
        super(cause);
    }
}
