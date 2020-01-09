/*
 *  Copyright 2018 Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.rest.query.QueryHandler;
import com.yahoo.bullet.rest.service.StatusService.TickQueryHandler;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class StatusServiceTest {
    @Test
    public void testBackendFailsAndSucceeds() {
        QueryService queryService = mock(QueryService.class);
        HandlerService handlerService = mock(HandlerService.class);
        doAnswer(invocationOnMock -> {
            ((QueryHandler) invocationOnMock.getArguments()[1]).fail(null);
            return null;
        }).when(handlerService).addHandler(anyString(), any());

        StatusService statusService = new StatusService(queryService, handlerService, 30000L, 10L, false);
        Assert.assertTrue(statusService.isBackendStatusOk());

        // <= 10 fails -> status ok
        for (int i = 0; i < 10; i++) {
            statusService.run();
        }
        Assert.assertTrue(statusService.isBackendStatusOk());
        verify(queryService, times(10)).submit(anyString(), eq(StatusService.TICK_QUERY));

        // > 10 fails (i.e. >= 10 retries) -> status not ok
        statusService.run();
        Assert.assertFalse(statusService.isBackendStatusOk());
        verify(queryService, times(11)).submit(anyString(), eq(StatusService.TICK_QUERY));

        doAnswer(invocationOnMock -> {
            ((QueryHandler) invocationOnMock.getArguments()[1]).send(null);
            return null;
        }).when(handlerService).addHandler(anyString(), any());

        // success -> status ok
        statusService.run();
        Assert.assertTrue(statusService.isBackendStatusOk());
        verify(queryService, times(12)).submit(anyString(), eq(StatusService.TICK_QUERY));
    }

    @Test
    public void testTickQueryHandlerSend() {
        TickQueryHandler queryHandler = new TickQueryHandler(30000L);

        // send completes result as true
        queryHandler.send(null);
        Assert.assertTrue(queryHandler.hasResult());

        // fail does not overwrite result
        queryHandler.fail(null);
        Assert.assertTrue(queryHandler.hasResult());
    }

    @Test
    public void testTickQueryHandlerFail() {
        TickQueryHandler queryHandler = new TickQueryHandler(30000L);

        // fail completes result as false
        queryHandler.fail(null);
        Assert.assertFalse(queryHandler.hasResult());

        // send does not overwrite result
        queryHandler.send(null);
        Assert.assertFalse(queryHandler.hasResult());
    }

    @Test
    public void testTickQueryHandlerTimeout() {
        TickQueryHandler queryHandler = new TickQueryHandler(0L);
        Assert.assertFalse(queryHandler.hasResult());
    }
}
