/*
 *  Copyright 2017 Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.rest.query.HTTPQueryHandler;
import com.yahoo.bullet.rest.query.QueryError;
import com.yahoo.bullet.rest.service.QueryService;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;

@SpringBootTest
public class QueryControllerTest extends AbstractTestNGSpringContextTests {
    @Autowired @InjectMocks
    private QueryController controller;
    @Mock
    private QueryService queryService;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testSend() throws Exception {
        String query = "foo";
        CompletableFuture<String> response = controller.submitQuery(query);

        ArgumentCaptor<HTTPQueryHandler> argument = ArgumentCaptor.forClass(HTTPQueryHandler.class);
        verify(queryService).submit(anyString(), eq(query), argument.capture());
        argument.getValue().send(new PubSubMessage("", "bar"));
        Assert.assertEquals(response.get(), "bar");
    }

    @Test
    public void testIllegalQuery() throws Exception {
        CompletableFuture<String> response = controller.submitQuery(null);

        ArgumentCaptor<HTTPQueryHandler> argument = ArgumentCaptor.forClass(HTTPQueryHandler.class);
        verify(queryService).submit(anyString(), eq("foo"), argument.capture());
        argument.getValue().send(new PubSubMessage("", "foo"));
        Assert.assertEquals(response.get(), QueryError.INVALID_QUERY.toString());
    }
}
