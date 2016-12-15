/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.rest.resource.DRPCResponse;
import org.springframework.stereotype.Service;

@Service
public interface DRPCService {
    /**
     * Returns the response of the DRPC call.
     *
     * @param request The input for the DRPC call.
     * @return The response from the DRPC call.
     */
    DRPCResponse invoke(String request);
}
