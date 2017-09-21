/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.rest.resource.QueryError;
import com.yahoo.bullet.rest.service.PubSubService;
import com.yahoo.bullet.rest.service.HTTPQueryHandler;
import com.yahoo.bullet.result.Clip;
import org.eclipse.jetty.http.HttpStatus;
import org.mockito.ArgumentCaptor;
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

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;

@ContextConfiguration(locations = "/TestApplicationContext.xml")
public class QueryControllerTest extends AbstractTestNGSpringContextTests {
    @Autowired @InjectMocks
    QueryController controller;

    @Mock
    PubSubService pubSubService;

    @BeforeMethod
    public void initMocks() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testRegisterAndSend() throws Exception {
        String randomQuery = "bar";
        AsyncResponse asyncResponse = Mockito.mock(AsyncResponse.class);
        ArgumentCaptor<Response> response = ArgumentCaptor.forClass(Response.class);
        Mockito.when(asyncResponse.resume(response.capture())).thenReturn(false);
        controller.submitQuery(randomQuery, asyncResponse);

        ArgumentCaptor<HTTPQueryHandler> argument = ArgumentCaptor.forClass(HTTPQueryHandler.class);
        Mockito.verify(pubSubService).submit(anyString(), eq(randomQuery), argument.capture());
        argument.getValue().send(new PubSubMessage("", "foo"));
        Assert.assertEquals(response.getValue().getEntity(), "foo");
    }

    @Test
    public void testIllegalQuery() {
        AsyncResponse asyncResponse = Mockito.mock(AsyncResponse.class);
        ArgumentCaptor<Response> response = ArgumentCaptor.forClass(Response.class);
        Mockito.when(asyncResponse.resume(response.capture())).thenReturn(false);
        controller.submitQuery(null, asyncResponse);

        Assert.assertEquals(response.getValue().getStatus(), HttpStatus.INTERNAL_SERVER_ERROR_500);
        QueryError cause = QueryError.INVALID_QUERY;
        Clip responseEntity = Clip.of(com.yahoo.bullet.result.Metadata.of(Error.makeError(cause.getError(), cause.getResolution())));
        Assert.assertEquals(response.getValue().getEntity(), responseEntity.asJSON());
    }
}
