/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.web.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;

@SpringBootApplication
@EnableAsync
public class Application extends SpringBootServletInitializer {
    /**
     * Main.
     *
     * @param args The arguments to the program.
     */
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
        return builder.sources(Application.class);
    }

    /**
     * Configures the Async executor.
     *
     * @return A {@link Executor} to use for async annotated code.
     */
    @Bean
    @ConfigurationProperties("threadPool")
    public Executor asyncExecutor() {
        return new ThreadPoolTaskExecutor();
    }

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
                registry.addMapping("/**");
            }
        };
    }

    /**
     * Creates a PubSub instance from a provided config.
     *
     * @param config The String path to the config file.
     * @return An instance of the particular {@link PubSub} indicated in the config.
     * @throws IOException if there were issues reading the config file as a {@link BulletConfig}.
     * @throws PubSubException if there were issues creating the PubSub instance.
     */
    @Bean
    public PubSub pubSub(@Value("${pubSub.config}") String config) throws IOException, PubSubException {
        return PubSub.from(new BulletConfig(config));
    }

    /**
     * Creates the specified number of {@link Publisher} instances from the given PubSub.
     *
     * @param pubSub The {@link PubSub} providing the instances.
     * @param publishers The number of publishers to create using {@link PubSub#getPublishers(int)}.
     * @return A {@link List} of {@link Publisher} provided by the {@link PubSub} instance.
     * @throws PubSubException if there were issues creating the instances.
     */
    @Bean
    public List<Publisher> publishers(PubSub pubSub, @Value("${pubSub.publishers}") int publishers) throws PubSubException {
        return pubSub.getPublishers(publishers);
    }

    /**
     * Creates the specified number of {@link Subscriber} instances from the given PubSub.
     *
     * @param pubSub The {@link PubSub} providing the instances.
     * @param subscribers The number of subscribers to create using {@link PubSub#getSubscribers(int)}.
     * @return A {@link List} of {@link Subscriber} provided by the {@link PubSub} instance.
     * @throws PubSubException if there were issues creating the instances.
     */
    @Bean
    public List<Subscriber> subscribers(PubSub pubSub, @Value("${pubSub.subscribers}") int subscribers) throws PubSubException {
        return pubSub.getSubscribers(subscribers);
    }
}
