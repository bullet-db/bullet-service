package com.yahoo.bullet.rest.model;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WebSocketResponse {

    public enum ResponseType {
        ACK,
        FAIL,
        CONTENT
    }

    @Setter
    @Getter
    private ResponseType type;

    @Setter @Getter
    private String content;
}
