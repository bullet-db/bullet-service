/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.AbstractWebSocketMessageBrokerConfigurer;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;

@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig extends AbstractWebSocketMessageBrokerConfigurer {
    private static final String ALLOW_ORIGINS = "*";

    @Value("${bullet.endpoint.websocket}")
    private String endpoint;
    @Value("${bullet.websocket.server.destination.prefix}")
    private String serverDestinationPrefix;
    @Value("${bullet.websocket.client.destination.prefix}")
    private String clientDestinationPrefix;
    @Value("${bullet.websocket.client.destination}")
    private String clientDestination;

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint(endpoint).setAllowedOrigins(ALLOW_ORIGINS).withSockJS();
    }

    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        registry.setApplicationDestinationPrefixes(serverDestinationPrefix);
        registry.enableSimpleBroker(clientDestination);
        registry.setUserDestinationPrefix(clientDestinationPrefix);
    }
}
