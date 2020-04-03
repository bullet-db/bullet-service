/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.bql.BulletQueryBuilder;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.metrics.MetricPublisher;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.rest.model.WebSocketRequest;
import com.yahoo.bullet.rest.model.WebSocketResponse;
import com.yahoo.bullet.rest.query.QueryError;
import com.yahoo.bullet.rest.service.BQLService;
import com.yahoo.bullet.rest.service.HandlerService;
import com.yahoo.bullet.rest.service.StatusService;
import com.yahoo.bullet.rest.service.WebSocketService;
import org.mockito.ArgumentCaptor;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.yahoo.bullet.TestHelpers.assertJSONEquals;
import static com.yahoo.bullet.TestHelpers.assertNoMetric;
import static com.yahoo.bullet.TestHelpers.assertOnlyMetricEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class WebSocketControllerTest {
    private WebSocketController controller;
    private WebSocketService webSocketService;
    private StatusService statusService;
    private BQLService bqlService;
    private MetricPublisher metricPublisher;

    private static final BulletQueryBuilder QUERY_BUILDER = new BulletQueryBuilder(new BulletConfig());

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
        doReturn(true).when(statusService).isBackendStatusOK();
        doReturn(false).when(statusService).queryLimitReached();

        webSocketService = mock(WebSocketService.class);
        bqlService = new BQLService(QUERY_BUILDER);
        metricPublisher = mock(MetricPublisher.class);
        controller = new WebSocketController(webSocketService, bqlService, statusService, metricPublisher);
    }

    @Test
    public void testWebSocketQueryWithBackendDown() {
        doReturn(false).when(statusService).isBackendStatusOK();

        String sessionID = "sessionID";
        WebSocketRequest request = getMockRequest(WebSocketRequest.Type.NEW_QUERY, "{}");
        SimpMessageHeaderAccessor headerAccessor = getMockMessageAccessor(sessionID);

        controller.submitWebsocketQuery(request, headerAccessor);

        ArgumentCaptor<WebSocketResponse> argument = ArgumentCaptor.forClass(WebSocketResponse.class);
        verify(webSocketService).sendResponse(eq(sessionID), argument.capture(), any());

        WebSocketResponse response = argument.getValue();
        Assert.assertEquals(response.getType(), WebSocketResponse.Type.FAIL);
        String expected = QueryError.SERVICE_UNAVAILABLE.toString();
        assertJSONEquals(response.getContent(), expected);
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.SERVICE_UNAVAILABLE), 1L);
    }

    @Test
    public void testSubmitNewQuery() {
        String query = "SELECT * FROM STREAM(1000, TIME) LIMIT 1;";
        WebSocketRequest request = getMockRequest(WebSocketRequest.Type.NEW_QUERY, query);
        String sessionID = "sessionID";
        SimpMessageHeaderAccessor headerAccessor = getMockMessageAccessor(sessionID);

        controller.submitWebsocketQuery(request, headerAccessor);
        ArgumentCaptor<Query> argument = ArgumentCaptor.forClass(Query.class);

        verify(webSocketService).submitQuery(anyString(), eq(sessionID), argument.capture(), any());
        Query actual = argument.getValue();

        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.CREATED), 1L);
        Assert.assertEquals(actual.getAggregation().getType(), Aggregation.Type.RAW);
        Assert.assertEquals(actual.getAggregation().getSize(), (Integer) 1);
        Assert.assertEquals(actual.getDuration(), (Long) 1000L);
        Assert.assertNull(actual.getFilter());
        Assert.assertNull(actual.getProjection());
        Assert.assertNull(actual.getWindow());
        Assert.assertNull(actual.getPostAggregations());
    }

    @Test
    public void testSubmitQueryTooManyQueries() {
        doReturn(true).when(statusService).queryLimitReached();

        String query = "SELECT * FROM STREAM(1000, TIME) LIMIT 1;";
        WebSocketRequest request = getMockRequest(WebSocketRequest.Type.NEW_QUERY, query);
        String sessionID = "sessionID";
        SimpMessageHeaderAccessor headerAccessor = getMockMessageAccessor(sessionID);

        controller.submitWebsocketQuery(request, headerAccessor);

        verify(webSocketService, never()).submitQuery(any(), any(), any(), any());
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.TOO_MANY_REQUESTS), 1L);
    }

    @Test
    public void testSubmitBadQuery() {
        String query = "SELECT * FROM STREAM(1000, TIME) WHERE 1 + 'foo';";
        WebSocketRequest request = getMockRequest(WebSocketRequest.Type.NEW_QUERY, query);
        String sessionID = "sessionID";
        SimpMessageHeaderAccessor headerAccessor = getMockMessageAccessor(sessionID);

        controller.submitWebsocketQuery(request, headerAccessor);

        verify(webSocketService, never()).submitQuery(any(), any(), any(), any());
        assertOnlyMetricEquals(controller.getMetricCollector(), metric(HttpStatus.BAD_REQUEST), 1L);
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
}
