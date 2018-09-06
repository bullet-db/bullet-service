/*
 *  Copyright 2017 Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.rest.query.HTTPQueryHandler;
import com.yahoo.bullet.rest.query.QueryError;
import com.yahoo.bullet.rest.query.SSEQueryHandler;
import com.yahoo.bullet.rest.service.QueryService;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
public class HTTPQueryControllerTest extends AbstractTestNGSpringContextTests {
    @Autowired
    @InjectMocks
    private HTTPQueryController controller;
    @Mock
    private QueryService service;
    @Autowired
    private WebApplicationContext context;
    private MockMvc mockMVC;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMVC = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @Test
    public void testSendHTTPQueryWithoutWindow() throws Exception {
        doReturn(0).when(service).runningQueryCount();
        String query = "{}";
        CompletableFuture<String> response = controller.submitHTTPQuery(query);

        ArgumentCaptor<HTTPQueryHandler> argument = ArgumentCaptor.forClass(HTTPQueryHandler.class);
        verify(service).submit(anyString(), eq(query), argument.capture());
        argument.getValue().send(new PubSubMessage("", "bar"));
        Assert.assertEquals(response.get(), "bar");
    }

    @Test
    public void testSendHTTPQueryWithNullWindow() throws Exception {
        doReturn(0).when(service).runningQueryCount();
        String query = "{\"window\": null}";
        CompletableFuture<String> response = controller.submitHTTPQuery(query);

        ArgumentCaptor<HTTPQueryHandler> argument = ArgumentCaptor.forClass(HTTPQueryHandler.class);
        verify(service).submit(anyString(), eq(query), argument.capture());
        argument.getValue().send(new PubSubMessage("", "bar"));
        Assert.assertEquals(response.get(), "bar");
    }

    @Test
    public void testSendHTTPQueryWithWindow() throws Exception {
        doReturn(0).when(service).runningQueryCount();
        String query = "{\"window\":{}}";
        CompletableFuture<String> response = controller.submitHTTPQuery(query);

        Assert.assertEquals(response.get(), QueryError.UNSUPPORTED_QUERY.toString());
    }

    @Test
    public void testSendHTTPTooManyQueries() throws Exception {
        doReturn(500).when(service).runningQueryCount();
        String query = "{}";
        CompletableFuture<String> response = controller.submitHTTPQuery(query);
        Assert.assertEquals(response.get(), "{\"records\":[],\"meta\":{\"errors\":[{\"error\":\"Too many concurrent queries in the system\",\"resolutions\":[\"Please try again later\"]}]}}");
    }

    @Test
    public void testInvalidQuery() throws Exception {
        String query = "invalid query";
        CompletableFuture<String> response = controller.submitHTTPQuery(query);

        String expected = "{\"records\":[],\"meta\":{\"errors\":[{\"error\":\"com.yahoo.bullet.bql.parser.ParsingException: line 1:1: missing \\u0027SELECT\\u0027 at \\u0027invalid\\u0027\",\"resolutions\":[\"Please provide a valid query.\"]}]}}";
        Assert.assertEquals(response.get(), expected);
    }

    @Test
    public void testInvalidQueryRuntimeException() throws Exception {
        doThrow(RuntimeException.class).when(service).submit(any(), any(), any());
        String query = "SELECT * FROM STREAM(30000, TIME) LIMIT 1;";
        CompletableFuture<String> response = controller.submitHTTPQuery(query);

        String expected = "{\"records\":[],\"meta\":{\"errors\":[{\"error\":\"Failed to parse query.\",\"resolutions\":[\"Please provide a valid query.\"]}]}}";
        Assert.assertEquals(response.get(), expected);
    }

    @Test
    public void testSSEQuery() throws Exception {
        doReturn(0).when(service).runningQueryCount();
        String query = "{foo}";

        MvcResult result = mockMVC.perform(post("/sse-query").contentType(MediaType.TEXT_PLAIN).content(query)).andReturn();

        ArgumentCaptor<SSEQueryHandler> argument = ArgumentCaptor.forClass(SSEQueryHandler.class);
        verify(service).submit(anyString(), eq(query), argument.capture());
        argument.getValue().send(new PubSubMessage("", "bar"));
        Assert.assertEquals(result.getResponse().getContentAsString(), "data:bar\n\n");

        argument.getValue().send(new PubSubMessage("", "baz"));
        Assert.assertEquals(result.getResponse().getContentAsString(), "data:bar\n\ndata:baz\n\n");
    }

    @Test
    public void testInvalidSSEQuery() throws Exception {
        String query = "invalid query";

        MvcResult result = mockMVC.perform(post("/sse-query").contentType(MediaType.TEXT_PLAIN).content(query)).andReturn();

        Assert.assertEquals(result.getResponse().getContentAsString(), "data:{\"records\":[],\"meta\":{\"errors\":[{\"error\":\"com.yahoo.bullet.bql.parser.ParsingException: line 1:1: missing \\u0027SELECT\\u0027 at \\u0027invalid\\u0027\",\"resolutions\":[\"Please provide a valid query.\"]}]}}\n\n");
    }

    @Test
    public void testSSEQueryTooManyQueries() throws Exception {
        doReturn(500).when(service).runningQueryCount();
        String query = "SELECT * FROM STREAM(30000, TIME) LIMIT 1;";

        MvcResult result = mockMVC.perform(post("/sse-query").contentType(MediaType.TEXT_PLAIN).content(query)).andReturn();

        Assert.assertEquals(result.getResponse().getContentAsString(), "data:{\"records\":[],\"meta\":{\"errors\":[{\"error\":\"Too many concurrent queries in the system\",\"resolutions\":[\"Please try again later\"]}]}}\n\n");
    }

    @Test
    public void testInvalidSSEQueryRuntimeException() throws Exception {
        doThrow(RuntimeException.class).when(service).submit(any(), any(), any());
        String query = "SELECT * FROM STREAM(30000, TIME) LIMIT 1;";

        MvcResult result = mockMVC.perform(post("/sse-query").contentType(MediaType.TEXT_PLAIN).content(query)).andReturn();

        Assert.assertEquals(result.getResponse().getContentAsString(), "data:{\"records\":[],\"meta\":{\"errors\":[{\"error\":\"Failed to parse query.\",\"resolutions\":[\"Please provide a valid query.\"]}]}}\n\n");
    }
}
