/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.rest.service.SchemaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@RestController
@Path("/columns")
public class SchemaController {
    public static final String JSON_API_HEADER = "application/vnd.api+json";
    @Autowired
    private SchemaService schemaService;

    /**
     * The GET endpoint that returns the JSON API schema.
     *
     * @return The JSON API schema.
     */
    @GET
    @Produces({ JSON_API_HEADER, MediaType.APPLICATION_JSON })
    public String getJSONSchema() {
        return schemaService.getSchema();
    }
}
