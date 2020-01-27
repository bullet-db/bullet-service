/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.common.metrics.MetricPublisher;
import com.yahoo.bullet.rest.model.WebSocketRequest;
import com.yahoo.bullet.rest.model.WebSocketResponse;
import com.yahoo.bullet.rest.service.HandlerService;
import com.yahoo.bullet.rest.service.PreprocessingService;
import com.yahoo.bullet.rest.service.StatusService;
import com.yahoo.bullet.rest.service.WebSocketService;
import org.mockito.ArgumentCaptor;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.yahoo.bullet.TestHelpers.assertJSONEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class WebSocketControllerTest {
    private WebSocketController controller;
    private WebSocketService webSocketService;
    private StatusService statusService;
    private HandlerService handlerService;
    private PreprocessingService preprocessingService;
    private MetricPublisher metricPublisher;

    private static SimpMessageHeaderAccessor getMockMessageAccessor(String sessionID) {
        SimpMessageHeaderAccessor headerAccessor = mock(SimpMessageHeaderAccessor.class);
        doReturn(sessionID).when(headerAccessor).getSessionId();
        return headerAccessor;
    }
    
    private static WebSocketRequest getMockRequest(WebSocketRequest.Type type, String content) {
        WebSocketRequest request = new WebSocketRequest();
        request.setType(type);
        request.setContent(content);
        return request;
    }
    
    @BeforeMethod
    public void setup() {
        statusService = mock(StatusService.class);
        doReturn(true).when(statusService).isBackendStatusOk();

        handlerService = mock(HandlerService.class);
        doReturn(0).when(handlerService).count();

        webSocketService = mock(WebSocketService.class);

        preprocessingService = new PreprocessingService(handlerService, 500);

        metricPublisher = mock(MetricPublisher.class);

        controller = new WebSocketController(webSocketService, preprocessingService, statusService, metricPublisher);
    }

    @Test
    public void testWebSocketQueryWithBackendDown() {
        doReturn(false).when(statusService).isBackendStatusOk();

        String sessionID = "sessionID";
        WebSocketRequest request = getMockRequest(WebSocketRequest.Type.NEW_QUERY, "{}");
        SimpMessageHeaderAccessor headerAccessor = getMockMessageAccessor(sessionID);

        controller.submitWebsocketQuery(request, headerAccessor);

        ArgumentCaptor<WebSocketResponse> argument = ArgumentCaptor.forClass(WebSocketResponse.class);
        verify(webSocketService).sendResponse(eq(sessionID), argument.capture(), any());

        WebSocketResponse response = argument.getValue();
        Assert.assertEquals(response.getType(), WebSocketResponse.Type.FAIL);
        String expected = "{'records':[],'meta':{'errors':[{'error':'Service temporarily unavailable','resolutions':['Please try again later']}]}}";
        assertJSONEquals(response.getContent(), expected);
    }

    @Test
    public void testSubmitNewQuery() {
        String query = "{}";
        WebSocketRequest request = getMockRequest(WebSocketRequest.Type.NEW_QUERY, query);
        String sessionID = "sessionID";
        SimpMessageHeaderAccessor headerAccessor = getMockMessageAccessor(sessionID);

        controller.submitWebsocketQuery(request, headerAccessor);

        verify(webSocketService).submitQuery(anyString(), eq(sessionID), eq(query), any());
    }

    @Test
    public void testSubmitQueryTooManyQueries() {
        doReturn(500).when(handlerService).count();

        String query = "{}";
        WebSocketRequest request = getMockRequest(WebSocketRequest.Type.NEW_QUERY, query);
        String sessionID = "sessionID";
        SimpMessageHeaderAccessor headerAccessor = getMockMessageAccessor(sessionID);

        controller.submitWebsocketQuery(request, headerAccessor);

        verify(webSocketService, never()).submitQuery(any(), any(), any(), any());
    }

    @Test
    public void testSubmitBadQuery() {
        String query = "This is a bad query";
        WebSocketRequest request = getMockRequest(WebSocketRequest.Type.NEW_QUERY, query);
        String sessionID = "sessionID";
        SimpMessageHeaderAccessor headerAccessor = getMockMessageAccessor(sessionID);

        controller.submitWebsocketQuery(request, headerAccessor);

        verify(webSocketService, never()).submitQuery(any(), any(), any(), any());
    }

    @Test
    public void testSubmitBadQueryRuntimeException() {
        WebSocketRequest request = mock(WebSocketRequest.class);
        doReturn(WebSocketRequest.Type.NEW_QUERY).when(request).getType();
        doThrow(new RuntimeException("Testing")).when(request).getContent();
        String sessionID = "sessionID";
        SimpMessageHeaderAccessor headerAccessor = getMockMessageAccessor(sessionID);

        controller.submitWebsocketQuery(request, headerAccessor);

        verify(webSocketService, never()).submitQuery(any(), any(), any(), any());
    }

    @Test
    public void testSubmitKillQuery() {
        String queryID = "queryID";
        WebSocketRequest request = getMockRequest(WebSocketRequest.Type.KILL_QUERY, queryID);
        String sessionID = "sessionID";
        SimpMessageHeaderAccessor headerAccessor = getMockMessageAccessor(sessionID);

        controller.submitWebsocketQuery(request, headerAccessor);

        verify(webSocketService).killQuery(eq(sessionID), eq(queryID));
    }
}
