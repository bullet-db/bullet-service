/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.rest.resource.DRPCError;
import com.yahoo.bullet.rest.resource.DRPCResponse;
import com.yahoo.bullet.rest.service.DRPCService;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response;

@ContextConfiguration(locations = "/TestApplicationContext.xml")
public class DRPCControllerTest extends AbstractTestNGSpringContextTests {
    @Autowired @InjectMocks
    DRPCController controller;

    @Mock
    private DRPCService drpcService;

    @BeforeMethod
    public void initMocks() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testDrpcCallGoodResponse() {
        Mockito.when(drpcService.invoke(Mockito.anyString())).thenReturn(new DRPCResponse("goodResponse"));
        Response response = controller.drpc("ignored");
        Assert.assertEquals(response.getStatusInfo(), Response.Status.OK);
        Assert.assertEquals(response.getEntity(), "goodResponse");
    }

    @Test
    public void testCannotReachDRPC() {
        DRPCError error = DRPCError.CANNOT_REACH_DRPC;
        Mockito.when(drpcService.invoke(Mockito.anyString())).thenReturn(new DRPCResponse(error));
        Response response = controller.drpc("ignored");
        Assert.assertEquals(response.getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
        String expected = Clip.of(Metadata.of(Error.makeError(error.getError(), error.getResolution()))).asJSON();
        Assert.assertEquals(expected, response.getEntity());
    }

    @Test
    public void testRetryLimitExceeded() {
        DRPCError error = DRPCError.RETRY_LIMIT_EXCEEDED;
        Mockito.when(drpcService.invoke(Mockito.anyString())).thenReturn(new DRPCResponse(error));
        Response response = controller.drpc("ignored");
        Assert.assertEquals(response.getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
        String expected = Clip.of(Metadata.of(Error.makeError(error.getError(), error.getResolution()))).asJSON();
        Assert.assertEquals(expected, response.getEntity());
    }
}
