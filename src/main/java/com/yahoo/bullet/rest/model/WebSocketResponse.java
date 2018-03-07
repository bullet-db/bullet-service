/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.model;

import lombok.Data;

@Data
public class WebSocketResponse {

    public enum ResponseType {
        ACK,
        FAIL,
        CONTENT
    }

    private ResponseType type;
    private String content;

    public WebSocketResponse(ResponseType type, String content) {
        this.type = type;
        this.content = content;
    }
}
