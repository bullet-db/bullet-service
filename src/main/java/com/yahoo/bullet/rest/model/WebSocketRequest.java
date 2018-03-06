package com.yahoo.bullet.rest.model;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WebSocketRequest {

    public enum RequestType {
        NEW_QUERY,
        KILL_QUERY
    }

    @Setter @Getter
    private RequestType type;

    @Setter @Getter
    private String content;

}
