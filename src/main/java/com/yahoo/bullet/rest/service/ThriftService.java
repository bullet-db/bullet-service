/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;
import com.yahoo.bullet.rest.resource.DRPCError;
import com.yahoo.bullet.rest.resource.DRPCResponse;
import com.yahoo.bullet.rest.utils.RandomPool;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
@NoArgsConstructor @Slf4j @Getter @Setter
public class ThriftService implements DRPCService {
    public static final String DRPC_SERVERS_KEY = "drpc.servers";
    public static final String DRPC_PORT_KEY = "drpc.port";

    private RandomPool<String> drpcServers;
    private int drpcPort;
    private String drpcFunction;
    private Map config;

    /**
     * Constructor that takes in the DRPC topology function name.
     *
     * @param drpcFunction The string that represents your DRPC endpoint.
     */
    public ThriftService(String drpcFunction) {
        // Till https://issues.apache.org/jira/browse/STORM-440 is resolved, need to load config.
        this.config = Utils.readStormConfig();
        this.drpcServers = new RandomPool((List<String>) config.get(DRPC_SERVERS_KEY));
        this.drpcPort = (Integer) config.get(DRPC_PORT_KEY);
        this.drpcFunction = drpcFunction;
    }

    /**
     * Creates a DRPCClient using the given parameters. Internal use only.
     *
     * @param config The configuration for the DRPCClient.
     * @param drpcServer The server to use.
     * @param drpcPort The port to use.
     * @return The created DRPCClient.
     * @throws Exception if the client cannot be initialized.
     */
    DRPCClient getClient(Map config, String drpcServer, int drpcPort) throws Exception {
        return new DRPCClient(config, drpcServer, drpcPort);
    }

    @Override
    public DRPCResponse invoke(String request) {
        String drpcServer = drpcServers.get();
        try {
            DRPCClient client = getClient(config, drpcServer, drpcPort);
            log.info("Using Thrift server: " + drpcServer + " with port: " + drpcPort + " for request: " + request);
            String response = client.execute(drpcFunction, request);
            log.info("Received response " + response);
            return new DRPCResponse(response);
        } catch (Exception e) {
            log.error("Thrift error ", e);
            return new DRPCResponse(DRPCError.CANNOT_REACH_DRPC);
        }
    }
}

