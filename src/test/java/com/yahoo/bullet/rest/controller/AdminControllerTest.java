/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.common.metrics.MetricPublisher;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.rest.common.Metric;
import com.yahoo.bullet.rest.service.QueryService;
import org.springframework.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AdminControllerTest {
    private static String metric(Metric metric) {
        return AdminController.STATUS_PREFIX + metric.toHTTPStatus();
    }

    @Test
    public void testExceptionOnSendingReplay() throws Exception {
        QueryService queryService = mock(QueryService.class);
        doThrow(new RuntimeException("Testing")).when(queryService).send(anyString(), any(Metadata.Signal.class));
        AdminController controller = new AdminController(queryService, mock(MetricPublisher.class));
        Assert.assertEquals(controller.sendReplay().get().getStatusCode(), HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @Test
    public void testExceptionWhileSendingReplay() throws Exception {
        QueryService queryService = mock(QueryService.class);
        CompletableFuture<PubSubMessage> fail = new CompletableFuture<>();
        fail.completeExceptionally(new RuntimeException("Testing"));
        doReturn(fail).when(queryService).send(anyString(), any(Metadata.Signal.class));
        AdminController controller = new AdminController(queryService, mock(MetricPublisher.class));
        Assert.assertEquals(controller.sendReplay().get().getStatusCode(), HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @Test
    public void testSendingReplay() throws Exception {
        QueryService queryService = mock(QueryService.class);
        doReturn(CompletableFuture.completedFuture(null)).when(queryService).send(anyString(), any(Metadata.Signal.class));
        AdminController controller = new AdminController(queryService, mock(MetricPublisher.class));
        Assert.assertEquals(controller.sendReplay().get().getStatusCode(), HttpStatus.OK);
        verify(queryService).send(anyString(), eq(Metadata.Signal.REPLAY));
    }
}
