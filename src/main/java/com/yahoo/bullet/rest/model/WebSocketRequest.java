/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
public class WebSocketRequest {

    public enum RequestType {
        NEW_QUERY,
        KILL_QUERY
    }

    @Getter @Setter
    private RequestType type;
    @Getter @Setter
    private String content;

    public WebSocketRequest(RequestType type, String content) {
        this.type = type;
        this.content = content;
    }
}
