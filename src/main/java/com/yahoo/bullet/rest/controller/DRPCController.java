/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.rest.resource.DRPCError;
import com.yahoo.bullet.rest.resource.DRPCResponse;
import com.yahoo.bullet.rest.service.DRPCService;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata;
import com.yahoo.bullet.parsing.Error;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@RestController
@Path("/drpc")
public class DRPCController {
    @Autowired
    private DRPCService drpcService;

    /**
     * The method that handles POSTs to this endpoint. Consumes and produces JSON.
     *
     * @param query The JSON query.
     * @return A {@link Response} object that is converted to JSON.
     */
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response drpc(String query) {
        DRPCResponse response = drpcService.invoke(query);
        if (response.hasError()) {
            DRPCError error = response.getError();
            Clip responseEntity = Clip.of(Metadata.of(Error.makeError(error.getError(), error.getResolution())));
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(responseEntity.asJSON()).build();
        }
        return Response.status(Response.Status.OK).entity(response.getContent()).build();
    }
}
