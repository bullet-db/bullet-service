/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.model;

import lombok.AllArgsConstructor;

import java.io.Serializable;

@AllArgsConstructor
public class QueryResponse implements Serializable {
    private static final long serialVersionUID = 437826438344200937L;

    private final String key;
    private final String id;
    private final String query;
    private final long createTime;
}
