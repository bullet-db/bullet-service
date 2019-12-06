/*
 *  Copyright 2018 Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.rest.query.QueryHandler;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class StatusServiceTest {
    @Test
    public void testBackendFailsAndSucceeds() {
        // query handler fails automatically
        HandlerService queryService = mock(HandlerService.class);
        doAnswer(invocationOnMock -> {
                ((QueryHandler) invocationOnMock.getArguments()[2]).fail(null);
                return null;
            }).when(queryService).submit(any(), any(), any());

        StatusService statusService = new StatusService(queryService, 30000L, 10L, false);
        Assert.assertTrue(statusService.isBackendStatusOk());

        // <= 10 fails -> status ok
        for (int i = 0; i < 10; i++) {
            statusService.run();
        }
        Assert.assertTrue(statusService.isBackendStatusOk());

        // > 10 fails (i.e. >= 10 retries) -> status not ok
        statusService.run();
        Assert.assertFalse(statusService.isBackendStatusOk());

        // query handler sends automatically
        doAnswer(invocationOnMock -> {
                ((QueryHandler) invocationOnMock.getArguments()[2]).send(null);
                return null;
            }).when(queryService).submit(any(), any(), any());

        // success -> status ok
        statusService.run();
        Assert.assertTrue(statusService.isBackendStatusOk());
    }

    @Test
    public void testTickQueryHandlerSend() {
        StatusService.TickQueryHandler queryHandler = new StatusService.TickQueryHandler(30000L);

        // send completes result as true
        queryHandler.send(null);
        Assert.assertTrue(queryHandler.hasResult());

        // fail does not overwrite result
        queryHandler.fail(null);
        Assert.assertTrue(queryHandler.hasResult());
    }

    @Test
    public void testTickQueryHandlerFail() {
        StatusService.TickQueryHandler queryHandler = new StatusService.TickQueryHandler(30000L);

        // fail completes result as false
        queryHandler.fail(null);
        Assert.assertFalse(queryHandler.hasResult());

        // send does not overwrite result
        queryHandler.send(null);
        Assert.assertFalse(queryHandler.hasResult());
    }

    @Test
    public void testTickQueryHandlerTimeout() {
        StatusService.TickQueryHandler queryHandler = new StatusService.TickQueryHandler(0L);
        Assert.assertFalse(queryHandler.hasResult());
    }
}
