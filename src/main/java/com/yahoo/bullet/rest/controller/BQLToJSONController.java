/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.rest.model.BQLToJSONResponse;
import com.yahoo.bullet.rest.service.PreprocessingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BQLToJSONController {
    private PreprocessingService preprocessingService;

    /**
     * Constructor that takes a {@link PreprocessingService}.
     *
     * @param preprocessingService The {@link PreprocessingService} to use.
     */
    @Autowired
    public BQLToJSONController(PreprocessingService preprocessingService) {
        this.preprocessingService = preprocessingService;
    }

    /**
     * The POST endpoint that converts the BQL query to the JSON Bullet query.
     *
     * @param query The BQL query string to be converted.
     * @return The JSON Bullet query or error message if failed.
     */
    @PostMapping(path = "${bullet.endpoint.bql-to-json}", consumes = {MediaType.TEXT_PLAIN_VALUE}, produces = {MediaType.APPLICATION_JSON_VALUE})
    public BQLToJSONResponse convertBQLToJSON(@RequestBody String query) {
        BQLToJSONResponse response = new BQLToJSONResponse();
        try {
            String json = preprocessingService.convertIfBQL(query);
            response.setHasError(false);
            response.setContent(json);
        } catch (Exception e) {
            response.setHasError(true);
            response.setContent(e.getMessage());
        }
        return response;
    }
}
