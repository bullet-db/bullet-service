/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Utilities;
import com.yahoo.bullet.pubsub.PubSubResponder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

@Configuration @Slf4j
@ConditionalOnProperty(prefix = "bullet.async", name = "enabled", havingValue = "true")
public class AsyncConfiguration {
    @RequiredArgsConstructor @Slf4j
    public static class ResponderClasses {
        private final BulletConfig config;
        @Getter @Setter
        private List<String> classes = new ArrayList<>();

        /**
         * Creates a {@link List} of {@link PubSubResponder} instantiated and configured classes out of the {@link List}
         * of String class names.
         *
         * @return The list of instantiated classes in the order provided.
         */
        List<PubSubResponder> create() {
            Objects.requireNonNull(config);
            Function<String, PubSubResponder> creator = createClass(config);
            return classes.stream().map(creator).collect(Collectors.toCollection(ArrayList::new));
        }

        private static Function<String, PubSubResponder> createClass(BulletConfig config) {
            return s -> {
                log.info("Loading PubSubResponder instance {}", s);
                return Utilities.loadConfiguredClass(s, config);
            };
        }
    }

    /**
     * Creates an instance of the {@link ResponderClasses}.
     *
     * @param config The String path to the config file.
     * @return The created instance.
     */
    @Bean @ConfigurationProperties("bullet.async.responders")
    public ResponderClasses responderClasses(@Value("${bullet.async.config}") String config) {
        return new ResponderClasses(new BulletConfig(config));
    }
}
