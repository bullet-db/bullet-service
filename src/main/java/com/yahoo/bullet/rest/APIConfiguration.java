/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest;

import org.springframework.boot.autoconfigure.security.oauth2.client.EnableOAuth2Sso;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

@Configuration
@EnableOAuth2Sso
public class APIConfiguration extends WebSecurityConfigurerAdapter {
    private static final String CORS_MAPPING = "/**";

    /**
     * Enables CORS globally.
     *
     * @return A {@link WebMvcConfigurer} instance with default CORS configuration - allow everything.
     */
    @Bean
    public WebMvcConfigurer corsConfigurer() {
        return new WebMvcConfigurerAdapter() {
            @Override
            public void addCorsMappings(CorsRegistry registry) {
                registry.addMapping(CORS_MAPPING);
            }
        };
    }

    @Override
    public void configure(HttpSecurity http) throws Exception {
        // Disable CSRF
        http.csrf().disable().authorizeRequests().anyRequest().authenticated();
    }
}
