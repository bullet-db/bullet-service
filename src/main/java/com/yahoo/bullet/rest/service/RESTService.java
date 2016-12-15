/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.rest.utils.RandomPool;
import com.yahoo.bullet.rest.resource.DRPCError;
import com.yahoo.bullet.rest.resource.DRPCResponse;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.springframework.stereotype.Service;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
@NoArgsConstructor @Slf4j @Getter @Setter
public class RESTService implements DRPCService {
    private RandomPool<String> urls;

    private int connectTimeout;
    private int retryLimit;

    public static final String URL_DELIMITER = ",";
    public static final String PATH_SEPARATOR = "/";
    public static final String PORT_PREFIX = ":";
    public static final String PROTOCOL_PREFIX = "http://";
    private static final String TEMPLATE = PROTOCOL_PREFIX + "%1$s" + PORT_PREFIX + "%2$s" + PATH_SEPARATOR + "%3$s";

    /**
     * Constructor that takes in a set of DRPC hosts separated by the
     * {@link RESTService#URL_DELIMITER}, the port to use for these urls
     * and the DRPC function name.
     *
     * @param urls The URL_DELIMITER separated set of hosts.
     * @param port The port to use.
     * @param path The path to use.
     *
     */
    public RESTService(String urls, String port, String path) {
        this.urls = new RandomPool(getURLs(urls, port, path));
    }

    private List<String> getURLs(@NonNull String urls, @NonNull String port, @NonNull String path) {
        return Stream.of(urls.split(URL_DELIMITER)).map(s -> String.format(TEMPLATE, s, port, path))
                     .collect(Collectors.toList());
    }

    private Client getClient() {
        ClientConfig config = new ClientConfig();
        config.property(ClientProperties.CONNECT_TIMEOUT, connectTimeout);
        return ClientBuilder.newClient(config);
    }

    /**
     * POSTs the request to the given url using the given client. Internal use only.
     *
     * @param client A client to use for making the call.
     * @param url The URL to make the call to.
     * @param request The String request to POST.
     * @return The response from the call.
     */
    DRPCResponse makeRequest(Client client, String url, String request) {
        log.info("Posting to " + url + " with \n" + request);
        WebTarget target = client.target(url);
        Response response = target.request(MediaType.TEXT_PLAIN).post(Entity.entity(request, MediaType.TEXT_PLAIN));
        Response.StatusType status = response.getStatusInfo();
        log.info("Received status code " + response.getStatus());
        String content = response.readEntity(String.class);
        log.info("Response content " + content);
        if (status.getFamily() == Response.Status.Family.SUCCESSFUL) {
            return new DRPCResponse(content);
        }
        return new DRPCResponse(DRPCError.CANNOT_REACH_DRPC);
    }

    private DRPCResponse tryRequest(String url, String request, Client client) {
        for (int i = 1; i <= retryLimit; ++i) {
            try {
                return makeRequest(client, url, request);
            } catch (ProcessingException pe) {
                log.warn("Attempt {} of {} failed for {}", i, retryLimit, url);
            }
        }
        throw new ProcessingException("Request to DRPC server " + url + " failed");
    }

    @Override
    public DRPCResponse invoke(String request) {
        Client client = getClient();
        String url = urls.get();
        try {
            return tryRequest(url, request, client);
        } catch (ProcessingException e) {
            log.warn("Retry limit exceeded. Could not reach DRPC endpoint url {}", url);
            return new DRPCResponse(DRPCError.RETRY_LIMIT_EXCEEDED);
        }
    }
}
