/*
 *  Copyright 2017 Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.rest.common.BQLException;
import com.yahoo.bullet.rest.query.HTTPQueryHandler;
import com.yahoo.bullet.rest.query.QueryError;
import com.yahoo.bullet.rest.query.SSEQueryHandler;
import com.yahoo.bullet.rest.service.HandlerService;
import com.yahoo.bullet.rest.service.PreprocessingService;
import com.yahoo.bullet.rest.service.QueryService;
import com.yahoo.bullet.rest.service.StatusService;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
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

import static com.yahoo.bullet.TestHelpers.assertJSONEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
public class HTTPQueryControllerTest extends AbstractTestNGSpringContextTests {
    @Autowired @InjectMocks
    private HTTPQueryController controller;
    @Mock
    private StatusService statusService;
    @Mock
    private PreprocessingService preprocessingService;
    @Mock
    private HandlerService handlerService;
    @Mock
    private QueryService queryService;

    @Autowired
    private WebApplicationContext context;
    private MockMvc mockMVC;

    private static void assertSSEJSONEquals(MvcResult result, String expected) throws Exception {
        String actual = result.getResponse().getContentAsString();
        Assert.assertTrue(actual.startsWith("data:"));
        String actualStripped = actual.substring(5);
        String expectedStripped = expected.substring(5);
        // Check end of record
        Assert.assertTrue(actualStripped.endsWith("\n\n"));
        // Check the JSON
        assertJSONEquals(actualStripped, expectedStripped);
    }

    @BeforeMethod
    public void setup() {
        initMocks(this);
        mockMVC = MockMvcBuilders.webAppContextSetup(context).build();
        doReturn(true).when(statusService).isBackendStatusOk();
        doReturn(false).when(preprocessingService).queryLimitReached();
    }

    @Test
    public void testSendHTTPQueryWithBackendDown() throws Exception {
        doReturn(false).when(statusService).isBackendStatusOk();
        String query = "{}";
        CompletableFuture<String> response = controller.submitHTTPQuery(query);
        String expected = "{'records':[],'meta':{'errors':[{'error':'Service temporarily unavailable'," +
                                                           "'resolutions':['Please try again later']}]}}";
        assertJSONEquals(response.get(), expected);
    }

    @Test
    public void testSendHTTPQueryWithoutWindow() throws Exception {
        String query = "{}";
        CompletableFuture<String> response = controller.submitHTTPQuery(query);
        ArgumentCaptor<HTTPQueryHandler> argument = ArgumentCaptor.forClass(HTTPQueryHandler.class);
        verify(handlerService).addHandler(anyString(), argument.capture());
        argument.getValue().send(new PubSubMessage("", "bar"));
        Assert.assertEquals(response.get(), "bar");
    }

    @Test
    public void testSendHTTPQueryWithNullWindow() throws Exception {
        String query = "{'window': null}";
        CompletableFuture<String> response = controller.submitHTTPQuery(query);
        ArgumentCaptor<HTTPQueryHandler> argument = ArgumentCaptor.forClass(HTTPQueryHandler.class);
        verify(handlerService).addHandler(anyString(), argument.capture());
        argument.getValue().send(new PubSubMessage("", "bar"));
        Assert.assertEquals(response.get(), "bar");
    }

    @Test
    public void testSendHTTPQueryWithWindow() throws Exception {
        doReturn(true).when(preprocessingService).containsWindow(anyString());
        String query = "{'window':{}}";
        CompletableFuture<String> response = controller.submitHTTPQuery(query);
        assertJSONEquals(response.get(), QueryError.UNSUPPORTED_QUERY.toString());
    }

    @Test
    public void testSendHTTPTooManyQueries() throws Exception {
        doReturn(true).when(preprocessingService).queryLimitReached();
        String query = "{}";
        CompletableFuture<String> response = controller.submitHTTPQuery(query);
        String expected = "{'records':[],'meta':{'errors':[{'error':'Too many concurrent queries in the system','resolutions':['Please try again later']}]}}";
        assertJSONEquals(response.get(), expected);
    }

    @Test
    public void testInvalidQuery() throws Exception {
        doThrow(new BQLException(new ParsingException("missing SELECT"))).when(preprocessingService).convertIfBQL(anyString());
        String query = "invalid query";
        CompletableFuture<String> response = controller.submitHTTPQuery(query);
        String expected = "{'records':[],'meta':{'errors':[{" +
            "'error':'com.yahoo.bullet.bql.parser.ParsingException: line 1:1: missing SELECT'," +
            "'resolutions':['Please provide a valid query']" +
            "}]}}";
        assertJSONEquals(response.get(), expected);
    }

    @Test
    public void testInvalidQueryRuntimeException() throws Exception {
        doThrow(RuntimeException.class).when(queryService).submit(anyString(), anyString());
        String query = "SELECT * FROM STREAM(30000, TIME) LIMIT 1;";
        CompletableFuture<String> response = controller.submitHTTPQuery(query);
        String expected = "{'records':[],'meta':{'errors':[{'error':'Failed to parse query','resolutions':['Please provide a valid query']}]}}";
        assertJSONEquals(response.get(), expected);
    }

    @Test
    public void testSSEQueryWithBackendDown() throws Exception {
        doReturn(false).when(statusService).isBackendStatusOk();
        String query = "{}";
        MvcResult result = mockMVC.perform(post("/sse-query").contentType(MediaType.TEXT_PLAIN).content(query)).andReturn();
        String expected = "data:{'records':[],'meta':{'errors':[{'error':'Service temporarily unavailable','resolutions':['Please try again later']}]}}\n\n";
        assertSSEJSONEquals(result, expected);
    }

    @Test
    public void testSSEQuery() throws Exception {
        String query = "{foo}";
        MvcResult result = mockMVC.perform(post("/sse-query").contentType(MediaType.TEXT_PLAIN).content(query)).andReturn();
        ArgumentCaptor<SSEQueryHandler> argument = ArgumentCaptor.forClass(SSEQueryHandler.class);
        verify(handlerService).addHandler(anyString(), argument.capture());
        argument.getValue().send(new PubSubMessage("", "bar"));
        Assert.assertEquals(result.getResponse().getContentAsString(), "data:bar\n\n");
        argument.getValue().send(new PubSubMessage("", "baz"));
        Assert.assertEquals(result.getResponse().getContentAsString(), "data:bar\n\ndata:baz\n\n");
    }

    @Test
    public void testInvalidSSEQuery() throws Exception {
        doThrow(new BQLException(new ParsingException("missing SELECT"))).when(preprocessingService).convertIfBQL(anyString());
        String query = "invalid query";
        MvcResult result = mockMVC.perform(post("/sse-query").contentType(MediaType.TEXT_PLAIN).content(query)).andReturn();
        String expected = "data:{'records':[],'meta':{'errors':[{" +
            "'error':'com.yahoo.bullet.bql.parser.ParsingException: line 1:1: missing SELECT'," +
            "'resolutions':['Please provide a valid query']}]}}\n\n";
        assertSSEJSONEquals(result, expected);
    }

    @Test
    public void testSSEQueryTooManyQueries() throws Exception {
        doReturn(true).when(preprocessingService).queryLimitReached();
        String query = "SELECT * FROM STREAM(30000, TIME) LIMIT 1;";
        MvcResult result = mockMVC.perform(post("/sse-query").contentType(MediaType.TEXT_PLAIN).content(query)).andReturn();
        String expected = "data:{'records':[],'meta':{'errors':[{'error':'Too many concurrent queries in the system','resolutions':['Please try again later']}]}}\n\n";
        assertSSEJSONEquals(result, expected);
    }

    @Test
    public void testInvalidSSEQueryRuntimeException() throws Exception {
        doThrow(RuntimeException.class).when(handlerService).addHandler(anyString(), any());
        String query = "SELECT * FROM STREAM(30000, TIME) LIMIT 1;";
        MvcResult result = mockMVC.perform(post("/sse-query").contentType(MediaType.TEXT_PLAIN).content(query)).andReturn();
        String expected = "data:{'records':[],'meta':{'errors':[{'error':'Failed to parse query','resolutions':['Please provide a valid query']}]}}\n\n";
        assertSSEJSONEquals(result, expected);
    }
}
