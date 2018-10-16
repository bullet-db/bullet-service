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

public class BackendStatusServiceTest {
    @Test
    public void testBackendFailsAndSucceeds() {
        // query handler fails automatically
        QueryService queryService = mock(QueryService.class);
        doAnswer(invocationOnMock -> {
                ((QueryHandler) invocationOnMock.getArguments()[2]).fail(null);
                return null;
            }).when(queryService).submit(any(), any(), any());

        BackendStatusService backendStatusService = new BackendStatusService(queryService, 30000L, 10L, false);
        Assert.assertTrue(backendStatusService.isBackendStatusOk());

        // <= 10 fails -> status ok
        for (int i = 0; i < 10; i++) {
            backendStatusService.run();
        }
        Assert.assertTrue(backendStatusService.isBackendStatusOk());

        // > 10 fails (i.e. >= 10 retries) -> status not ok
        backendStatusService.run();
        Assert.assertFalse(backendStatusService.isBackendStatusOk());

        // query handler sends automatically
        doAnswer(invocationOnMock -> {
                ((QueryHandler) invocationOnMock.getArguments()[2]).send(null);
                return null;
            }).when(queryService).submit(any(), any(), any());

        // success -> status ok
        backendStatusService.run();
        Assert.assertTrue(backendStatusService.isBackendStatusOk());
    }

    @Test
    public void testTickQueryHandlerSend() {
        BackendStatusService.TickQueryHandler queryHandler = new BackendStatusService.TickQueryHandler(30000L);

        // send completes result as true
        queryHandler.send(null);
        Assert.assertTrue(queryHandler.hasResult());

        // fail does not overwrite result
        queryHandler.fail(null);
        Assert.assertTrue(queryHandler.hasResult());
    }

    @Test
    public void testTickQueryHandlerFail() {
        BackendStatusService.TickQueryHandler queryHandler = new BackendStatusService.TickQueryHandler(30000L);

        // fail completes result as false
        queryHandler.fail(null);
        Assert.assertFalse(queryHandler.hasResult());

        // send does not overwrite result
        queryHandler.send(null);
        Assert.assertFalse(queryHandler.hasResult());
    }

    @Test
    public void testTickQueryHandlerTimeout() {
        BackendStatusService.TickQueryHandler queryHandler = new BackendStatusService.TickQueryHandler(0L);
        Assert.assertFalse(queryHandler.hasResult());
    }
}
