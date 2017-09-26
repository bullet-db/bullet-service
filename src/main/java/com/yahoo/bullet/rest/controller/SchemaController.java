/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.rest.service.SchemaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SchemaController {
    public static final String JSON_API_HEADER = "application/vnd.api+json";
    @Autowired
    private SchemaService schemaService;

    /**
     * The GET endpoint that returns the JSON API schema.
     *
     * @return The JSON API schema.
     */
    @GetMapping(path = "/columns", produces = { JSON_API_HEADER, MediaType.APPLICATION_JSON_VALUE })
    public String getJSONSchema() {
        return schemaService.getSchema();
    }
}
