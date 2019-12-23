/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.PubSubResponder;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

@Configuration @Slf4j
@ConditionalOnProperty(prefix = "bullet.async", name = "enable", havingValue = "true")
public class AsyncConfiguration {
    @NoArgsConstructor @AllArgsConstructor @Slf4j
    public static class ResponderClasses {
        @Getter @Setter
        private List<String> classes = new ArrayList<>();
        private static final String CONFIG_KEY = "bullet.async.configuration.class.name";

        /**
         * Creates a {@link List} of {@link PubSubResponder} instantiated and configured classes out of the {@link List}
         * of String class names.
         *
         * @param config The {@link BulletConfig} to instantiate the classes with. It should contain the superset of
         *               all required configs needed by all the responder classes.
         * @return The list of instantiated classes in the order provided.
         */
        List<PubSubResponder> create(BulletConfig config) {
            Objects.requireNonNull(config);
            Function<String, PubSubResponder> creator = createClass(config);
            return classes.stream().map(creator).collect(Collectors.toList());
        }

        private static Function<String, PubSubResponder> createClass(BulletConfig config) {
            return s -> {
                log.info("Loading PubSubResponder instance {}", s);
                config.set(CONFIG_KEY, s);
                return config.loadConfiguredClass(CONFIG_KEY);
            };
        }
    }

    /**
     * Creates an instance of the {@link ResponderClasses}.
     *
     * @return The created instance.
     */
    @Bean @ConfigurationProperties("bullet.async.responders")
    public ResponderClasses responderClasses() {
        return new ResponderClasses();
    }

    /**
     * Loads the async configuration using the provided path to the configuration file.
     *
     * @param config The String path to the file.
     * @return The created configuration for the async responders.
     */
    @Bean
    public BulletConfig asyncConfig(@Value("${bullet.async.config}") String config) {
        return new BulletConfig(config);
    }

    /**
     * Loads custom responders to use for asynchronous queries.
     *
     * @param asyncConfig The {@link BulletConfig} with the superset of all the settings needed by all the responders.
     * @param responderClasses A {@link ResponderClasses} instances containing the names of classes.
     * @return A {@link List} of {@link PubSubResponder} instances.
     */
    @Bean
    public List<PubSubResponder> asyncResponders(BulletConfig asyncConfig, ResponderClasses responderClasses) {
        if (responderClasses == null) {
            log.warn("Async results are enabled but no configured instances of responders were found!");
            return Collections.emptyList();
        }
        return responderClasses.create(asyncConfig);
    }
}
