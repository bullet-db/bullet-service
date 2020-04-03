/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import static com.yahoo.bullet.TestHelpers.assertJSONEquals;
import static com.yahoo.bullet.TestHelpers.assertOnlyMetricEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class WebSocketControllerTest {
    /*
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


    private static String metric(HttpStatus httpStatus) {
        return WebSocketController.STATUS_PREFIX + httpStatus;
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
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.SERVICE_UNAVAILABLE), 1L);
    }

    @Test
    public void testSubmitNewQuery() {
        String query = "{}";
        WebSocketRequest request = getMockRequest(WebSocketRequest.Type.NEW_QUERY, query);
        String sessionID = "sessionID";
        SimpMessageHeaderAccessor headerAccessor = getMockMessageAccessor(sessionID);

        controller.submitWebsocketQuery(request, headerAccessor);

        verify(webSocketService).submitQuery(anyString(), eq(sessionID), eq(query), any());
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.CREATED), 1L);
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
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.TOO_MANY_REQUESTS), 1L);
    }

    @Test
    public void testSubmitBadQuery() {
        String query = "This is a bad query";
        WebSocketRequest request = getMockRequest(WebSocketRequest.Type.NEW_QUERY, query);
        String sessionID = "sessionID";
        SimpMessageHeaderAccessor headerAccessor = getMockMessageAccessor(sessionID);

        controller.submitWebsocketQuery(request, headerAccessor);

        verify(webSocketService, never()).submitQuery(any(), any(), any(), any());
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.BAD_REQUEST), 1L);
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
        assertNoMetric(controller.getMetricCollector().extractMetrics());
    }
     */
}
