/*
 *  Copyright 2017 Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.rest.query.HTTPQueryHandler;
import com.yahoo.bullet.rest.query.QueryError;
import com.yahoo.bullet.rest.query.SSEQueryHandler;
import com.yahoo.bullet.rest.service.BQLService;
import com.yahoo.bullet.rest.service.HandlerService;
import com.yahoo.bullet.rest.service.QueryService;
import com.yahoo.bullet.rest.service.StatusService;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
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
import static com.yahoo.bullet.TestHelpers.assertOnlyMetricEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
public class HTTPQueryControllerTest extends AbstractTestNGSpringContextTests {
    /*
    @Autowired
    @InjectMocks
    private HTTPQueryController controller;
    @Mock
    private StatusService statusService;
    @Mock
    private BQLService bqlService;
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

    private static String metric(HttpStatus httpStatus) {
        return HTTPQueryController.STATUS_PREFIX + httpStatus;
    }

    @BeforeMethod
    public void setup() {
        initMocks(this);
        mockMVC = MockMvcBuilders.webAppContextSetup(context).build();
        doReturn(true).when(statusService).isBackendStatusOK();
        doReturn(false).when(statusService).queryLimitReached();
    }

    @Test
    public void testSubmitHTTPQueryWithBackendDown() throws Exception {
        doReturn(false).when(statusService).isBackendStatusOK();
        Query query = "{}";
        CompletableFuture<String> response = controller.submitHTTPQuery(query);
        String expected = "{'records':[],'meta':{'errors':[{'error':'Service temporarily unavailable'," +
                                                           "'resolutions':['Please try again later']}]}}";
        assertJSONEquals(response.get(), expected);
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.SERVICE_UNAVAILABLE), 1L);
    }

    @Test
    public void testSubmitHTTPQueryWithoutWindow() throws Exception {
        Query query = "{}";
        CompletableFuture<String> response = controller.submitHTTPQuery(query);
        ArgumentCaptor<HTTPQueryHandler> argument = ArgumentCaptor.forClass(HTTPQueryHandler.class);
        verify(handlerService).addHandler(anyString(), argument.capture());
        argument.getValue().send(new PubSubMessage("", "bar"));
        Assert.assertEquals(response.get(), "bar");
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.CREATED), 1L);
    }

    @Test
    public void testSubmitHTTPQueryWithNullWindow() throws Exception {
        Query query = "{'window': null}";
        CompletableFuture<String> response = controller.submitHTTPQuery(query);
        ArgumentCaptor<HTTPQueryHandler> argument = ArgumentCaptor.forClass(HTTPQueryHandler.class);
        verify(handlerService).addHandler(anyString(), argument.capture());
        argument.getValue().send(new PubSubMessage("", "bar"));
        Assert.assertEquals(response.get(), "bar");
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.CREATED), 1L);
    }

    @Test
    public void testSubmitHTTPQueryWithWindow() throws Exception {
        doReturn(true).when(bqlService).containsWindow(anyString());
        Query query = "{'window':{}}";
        CompletableFuture<String> response = controller.submitHTTPQuery(query);
        assertJSONEquals(response.get(), QueryError.UNSUPPORTED_QUERY.toString());
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.BAD_REQUEST), 1L);
    }

    @Test
    public void testSubmitHTTPQueryInBQL() throws Exception {
        doReturn(false).when(bqlService).containsWindow(anyString());
        doAnswer(i -> i.getArgumentAt(0, String.class)).when(bqlService).toQuery(anyString());
        Query query = "bql query without window";
        CompletableFuture<String> response = controller.submitHTTPQuery(query);
        ArgumentCaptor<HTTPQueryHandler> argument = ArgumentCaptor.forClass(HTTPQueryHandler.class);
        verify(handlerService).addHandler(anyString(), argument.capture());
        verify(queryService).submit(anyString(), eq("bql query without window"));
        argument.getValue().send(new PubSubMessage("", "bar"));
        Assert.assertEquals(response.get(), "bar");
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.CREATED), 1L);
    }

    @Test
    public void testSubmitHTTPQueryWhenTooManyQueries() throws Exception {
        doReturn(true).when(statusService).queryLimitReached();
        Query query = "{}";
        CompletableFuture<String> response = controller.submitHTTPQuery(query);
        String expected = "{'records':[],'meta':{'errors':[{'error':'Too many concurrent queries in the system','resolutions':['Please try again later']}]}}";
        assertJSONEquals(response.get(), expected);
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.TOO_MANY_REQUESTS), 1L);
    }

    @Test
    public void testSubmitInvalidBQLHTTPQuery() throws Exception {
        doThrow(new BQLException(new ParsingException("missing SELECT"))).when(bqlService).toQuery(anyString());
        Query query = "invalid query";
        CompletableFuture<String> response = controller.submitHTTPQuery(query);
        String expected = "{'records':[],'meta':{'errors':[{" +
            "'error':'com.yahoo.bullet.bql.parser.ParsingException: line 1:1: missing SELECT'," +
            "'resolutions':['Please provide a valid query']" +
            "}]}}";
        assertJSONEquals(response.get(), expected);
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.BAD_REQUEST), 1L);
    }

    @Test
    public void testSubmitInvalidHTTPQuery() throws Exception {
        doThrow(RuntimeException.class).when(queryService).submit(anyString(), anyString());
        Query query = "SELECT * FROM STREAM(30000, TIME) LIMIT 1;";
        CompletableFuture<String> response = controller.submitHTTPQuery(query);
        String expected = "{'records':[],'meta':{'errors':[{'error':'Failed to parse query','resolutions':['Please provide a valid query']}]}}";
        assertJSONEquals(response.get(), expected);
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.BAD_REQUEST), 1L);
    }

    @Test
    public void testSubmitSSEQueryWithBackendDown() throws Exception {
        doReturn(false).when(statusService).isBackendStatusOK();
        String query = "{}";
        MvcResult result = mockMVC.perform(post("/sse-query").contentType(MediaType.TEXT_PLAIN).content(query)).andReturn();
        String expected = "data:{'records':[],'meta':{'errors':[{'error':'Service temporarily unavailable','resolutions':['Please try again later']}]}}\n\n";
        assertSSEJSONEquals(result, expected);
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.SERVICE_UNAVAILABLE), 1L);
    }

    @Test
    public void testSubmitSSEQuery() throws Exception {
        String query = "{foo}";
        MvcResult result = mockMVC.perform(post("/sse-query").contentType(MediaType.TEXT_PLAIN).content(query)).andReturn();
        ArgumentCaptor<SSEQueryHandler> argument = ArgumentCaptor.forClass(SSEQueryHandler.class);
        verify(handlerService).addHandler(anyString(), argument.capture());
        argument.getValue().send(new PubSubMessage("", "bar"));
        Assert.assertEquals(result.getResponse().getContentAsString(), "data:bar\n\n");
        argument.getValue().send(new PubSubMessage("", "baz"));
        Assert.assertEquals(result.getResponse().getContentAsString(), "data:bar\n\ndata:baz\n\n");
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.CREATED), 1L);
    }

    @Test
    public void testSubmitInvalidBQLSSEQuery() throws Exception {
        doThrow(new BQLException(new ParsingException("missing SELECT"))).when(bqlService).toQuery(anyString());
        String query = "invalid query";
        MvcResult result = mockMVC.perform(post("/sse-query").contentType(MediaType.TEXT_PLAIN).content(query)).andReturn();
        String expected = "data:{'records':[],'meta':{'errors':[{" +
            "'error':'com.yahoo.bullet.bql.parser.ParsingException: line 1:1: missing SELECT'," +
            "'resolutions':['Please provide a valid query']}]}}\n\n";
        assertSSEJSONEquals(result, expected);
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.BAD_REQUEST), 1L);
    }

    @Test
    public void testSubmitInvalidSSEQuery() throws Exception {
        doThrow(RuntimeException.class).when(handlerService).addHandler(anyString(), any());
        String query = "SELECT * FROM STREAM(30000, TIME) LIMIT 1;";
        MvcResult result = mockMVC.perform(post("/sse-query").contentType(MediaType.TEXT_PLAIN).content(query)).andReturn();
        String expected = "data:{'records':[],'meta':{'errors':[{'error':'Failed to parse query','resolutions':['Please provide a valid query']}]}}\n\n";
        assertSSEJSONEquals(result, expected);
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.BAD_REQUEST), 1L);
    }

    @Test
    public void testSubmitSSEQueryWhenTooManyQueries() throws Exception {
        doReturn(true).when(bqlService).queryLimitReached();
        String query = "SELECT * FROM STREAM(30000, TIME) LIMIT 1;";
        MvcResult result = mockMVC.perform(post("/sse-query").contentType(MediaType.TEXT_PLAIN).content(query)).andReturn();
        String expected = "data:{'records':[],'meta':{'errors':[{'error':'Too many concurrent queries in the system','resolutions':['Please try again later']}]}}\n\n";
        assertSSEJSONEquals(result, expected);
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.TOO_MANY_REQUESTS), 1L);
    }

    @Test
    public void testSubmitAsyncQuery() throws Exception {
        doAnswer(i -> i.getArgumentAt(0, String.class)).when(bqlService).toQuery(anyString());
        doAnswer(i -> {
            String id = i.getArgumentAt(0, String.class);
            String query = i.getArgumentAt(1, String.class);
            return CompletableFuture.completedFuture(new PubSubMessage(id, query));
        }).when(queryService).submit(anyString(), anyString());

        long start = System.currentTimeMillis();
        ResponseEntity<Object> response = controller.submitAsyncQuery("query").get();
        long end = System.currentTimeMillis();

        Assert.assertNotNull((response));
        Assert.assertEquals(response.getStatusCode(), HttpStatus.CREATED);
        QueryResponse queryResponse = (QueryResponse) response.getBody();
        verify(queryService).submit(eq(queryResponse.getId()), eq("query"));
        Assert.assertEquals(queryResponse.getQuery(), "query");
        Assert.assertTrue(queryResponse.getCreateTime() >= start && queryResponse.getCreateTime() <= end);
        verifyZeroInteractions(handlerService);
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.CREATED), 1L);
    }

    @Test
    public void testSubmitInvalidBQLAsyncQuery() throws Exception {
        doThrow(new BQLException(new ParsingException("missing SELECT"))).when(bqlService).toQuery(anyString());

        ResponseEntity<Object> response = controller.submitAsyncQuery("query").get();

        Assert.assertNotNull((response));
        Assert.assertEquals(response.getStatusCode(), HttpStatus.BAD_REQUEST);
        verifyZeroInteractions(queryService);
        String expected = "{'records':[],'meta':{'errors':[{" +
            "'error':'com.yahoo.bullet.bql.parser.ParsingException: line 1:1: missing SELECT'," +
            "'resolutions':['Please provide a valid query']" +
            "}]}}";
        assertJSONEquals(response.getBody().toString(), expected);
        verifyZeroInteractions(handlerService);
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.BAD_REQUEST), 1L);
    }

    @Test
    public void testSubmitInvalidAsyncQuery() throws Exception {
        doThrow(new RuntimeException("Testing")).when(bqlService).toQuery(anyString());

        ResponseEntity<Object> response = controller.submitAsyncQuery("query").get();

        Assert.assertNotNull((response));
        Assert.assertEquals(response.getStatusCode(), HttpStatus.BAD_REQUEST);
        QueryError queryError = (QueryError) response.getBody();
        assertJSONEquals(queryError.toString(), QueryError.INVALID_QUERY.toString());
        verifyZeroInteractions(queryService);
        verifyZeroInteractions(handlerService);
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.BAD_REQUEST), 1L);
    }

    @Test
    public void testSubmitAsyncQueryWithBackendDown() throws Exception {
        doReturn(false).when(statusService).isBackendStatusOK();
        ResponseEntity<Object> response = controller.submitAsyncQuery("query").get();
        Assert.assertNotNull((response));
        Assert.assertEquals(response.getStatusCode(), HttpStatus.SERVICE_UNAVAILABLE);
        QueryError queryError = (QueryError) response.getBody();
        assertJSONEquals(queryError.toString(), QueryError.SERVICE_UNAVAILABLE.toString());
        verifyZeroInteractions(queryService);
        verifyZeroInteractions(handlerService);
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.SERVICE_UNAVAILABLE), 1L);
    }

    @Test
    public void testSubmitAsyncQueryWhenCannotPublish() throws Exception {
        doAnswer(i -> i.getArgumentAt(0, String.class)).when(bqlService).toQuery(anyString());
        doReturn(CompletableFuture.completedFuture(null)).when(queryService).submit(anyString(), anyString());

        ResponseEntity<Object> response = controller.submitAsyncQuery("query").get();
        Assert.assertNotNull((response));
        Assert.assertEquals(response.getStatusCode(), HttpStatus.INTERNAL_SERVER_ERROR);
        QueryError queryError = (QueryError) response.getBody();
        assertJSONEquals(queryError.toString(), QueryError.SERVICE_UNAVAILABLE.toString());
        verify(queryService).submit(anyString(), eq("query"));
        verifyZeroInteractions(handlerService);
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.INTERNAL_SERVER_ERROR), 1L);
    }

    @Test
    public void testSubmitAsyncQueryWhenResolvingToError() throws Exception {
        doAnswer(i -> i.getArgumentAt(0, String.class)).when(bqlService).toQuery(anyString());
        CompletableFuture<PubSubMessage> fail = new CompletableFuture<>();
        fail.completeExceptionally(new RuntimeException("Testing"));
        doReturn(fail).when(queryService).submit(anyString(), anyString());

        ResponseEntity<Object> response = controller.submitAsyncQuery("query").get();
        Assert.assertNotNull((response));
        Assert.assertEquals(response.getStatusCode(), HttpStatus.INTERNAL_SERVER_ERROR);
        QueryError queryError = (QueryError) response.getBody();
        assertJSONEquals(queryError.toString(), QueryError.SERVICE_UNAVAILABLE.toString());
        verify(queryService).submit(anyString(), eq("query"));
        verifyZeroInteractions(handlerService);
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.INTERNAL_SERVER_ERROR), 1L);
    }

    @Test
    public void testDeletingAsyncQuery() throws Exception {
        doReturn(CompletableFuture.completedFuture(null)).when(queryService).kill(anyString());
        ResponseEntity<Object> response = controller.deleteAsyncQuery("id").get();
        Assert.assertNotNull((response));
        Assert.assertEquals(response.getStatusCode(), HttpStatus.OK);
        Assert.assertNull((response.getBody()));
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.OK), 1L);
    }

    @Test
    public void testDeletingAsyncQueryWithBackendDown() throws Exception {
        doReturn(false).when(statusService).isBackendStatusOK();
        ResponseEntity<Object> response = controller.deleteAsyncQuery("id").get();
        Assert.assertNotNull((response));
        Assert.assertEquals(response.getStatusCode(), HttpStatus.SERVICE_UNAVAILABLE);
        QueryError queryError = (QueryError) response.getBody();
        assertJSONEquals(queryError.toString(), QueryError.SERVICE_UNAVAILABLE.toString());
        verifyZeroInteractions(queryService);
        verifyZeroInteractions(handlerService);
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.SERVICE_UNAVAILABLE), 1L);
    }

    @Test
    public void testDeletingAsyncQueryResultingInError() throws Exception {
        doThrow(new RuntimeException("Testing")).when(queryService).kill(anyString());
        ResponseEntity<Object> response = controller.deleteAsyncQuery("id").get();
        Assert.assertNotNull((response));
        Assert.assertEquals(response.getStatusCode(), HttpStatus.INTERNAL_SERVER_ERROR);
        QueryError queryError = (QueryError) response.getBody();
        assertJSONEquals(queryError.toString(), QueryError.SERVICE_UNAVAILABLE.toString());
        verify(queryService).kill(eq("id"));
        verifyZeroInteractions(handlerService);
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.INTERNAL_SERVER_ERROR), 1L);
    }

    @Test
    public void testDeleteAsyncQueryWhenResolvingToError() throws Exception {
        CompletableFuture<Void> fail = new CompletableFuture<>();
        fail.completeExceptionally(new RuntimeException("Testing"));
        doReturn(fail).when(queryService).kill(anyString());

        ResponseEntity<Object> response = controller.deleteAsyncQuery("id").get();
        Assert.assertNotNull((response));
        Assert.assertEquals(response.getStatusCode(), HttpStatus.INTERNAL_SERVER_ERROR);
        QueryError queryError = (QueryError) response.getBody();
        assertJSONEquals(queryError.toString(), QueryError.SERVICE_UNAVAILABLE.toString());
        verify(queryService).kill(eq("id"));
        verifyZeroInteractions(handlerService);
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.INTERNAL_SERVER_ERROR), 1L);
    }
    
     */
}
