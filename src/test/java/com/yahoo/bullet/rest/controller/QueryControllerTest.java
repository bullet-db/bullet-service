/*
 *  Copyright 2017 Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.rest.model.WebSocketRequest;
import com.yahoo.bullet.rest.query.HTTPQueryHandler;
import com.yahoo.bullet.rest.query.QueryError;
import com.yahoo.bullet.rest.query.SSEQueryHandler;
import com.yahoo.bullet.rest.service.QueryService;
import com.yahoo.bullet.rest.service.WebSocketService;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessageType;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
public class QueryControllerTest extends AbstractTestNGSpringContextTests {
    @Autowired @InjectMocks
    private QueryController controller;
    @Mock
    private QueryService service;
    @Mock
    private WebSocketService webSocketService;
    @Autowired
    private WebApplicationContext context;
    private MockMvc mockMvc;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @Test
    public void testSendByHTTP() throws Exception {
        String query = "foo";
        CompletableFuture<String> response = controller.submitQuery(query);

        ArgumentCaptor<HTTPQueryHandler> argument = ArgumentCaptor.forClass(HTTPQueryHandler.class);
        verify(service).submit(anyString(), eq(query), argument.capture());
        argument.getValue().send(new PubSubMessage("", "bar"));
        Assert.assertEquals(response.get(), "bar");
    }

    @Test
    public void testIllegalQueryByHTTP() throws Exception {
        CompletableFuture<String> response = controller.submitQuery(null);

        Assert.assertEquals(response.get(), QueryError.INVALID_QUERY.toString());
    }

    @Test
    public void testSSEQuery() throws Exception {
        String query = "foo";

        MvcResult result = mockMvc.perform(
                post("/querySSE")
                        .contentType(MediaType.TEXT_PLAIN)
                        .content(query)
        ).andReturn();

        ArgumentCaptor<SSEQueryHandler> argument = ArgumentCaptor.forClass(SSEQueryHandler.class);
        verify(service).submit(anyString(), eq(query), argument.capture());
        argument.getValue().send(new PubSubMessage("", "bar"));
        Assert.assertEquals(result.getResponse().getContentAsString(), "data:bar\n\n");

        argument.getValue().send(new PubSubMessage("", "baz"));
        Assert.assertEquals(result.getResponse().getContentAsString(), "data:bar\n\ndata:baz\n\n");
    }

    @Test
    public void testSubmitNewQueryByWebSocket() {
        WebSocketRequest request = new WebSocketRequest();
        request.setType(WebSocketRequest.RequestType.NEW_QUERY);
        request.setContent("foo");
        SimpMessageHeaderAccessor headerAccessor = SimpMessageHeaderAccessor.create(SimpMessageType.MESSAGE);

        controller.submitWebsocketQuery(request, headerAccessor);

        verify(webSocketService).submitQuery(anyString(), eq("foo"), eq(headerAccessor));
    }

    @Test
    public void testSubmitKillQueryByWebSocket() {
        WebSocketRequest request = new WebSocketRequest();
        request.setType(WebSocketRequest.RequestType.KILL_QUERY);
        request.setContent("queryID");
        SimpMessageHeaderAccessor headerAccessor = mock(SimpMessageHeaderAccessor.class);
        when(headerAccessor.getSessionId()).thenReturn("sessionID");

        controller.submitWebsocketQuery(request, headerAccessor);

        verify(webSocketService).sendKillSignal(eq("sessionID"), eq("queryID"));
    }
}
