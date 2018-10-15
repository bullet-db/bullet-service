/*
 *  Copyright 2018 Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.rest.query.QueryHandler;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class BackendStatusServiceTest {
    @Test
    public void testBackendSucceeds() {
        // Succeeding sets the backend status to ok
        QueryService queryService = mock(QueryService.class);
        doAnswer(invocationOnMock -> {
                ((QueryHandler) invocationOnMock.getArguments()[2]).send(null);
                return null;
            }).when(queryService).submit(any(), any(), any());

        BackendStatusService backendStatusService = new BackendStatusService(queryService, 1L, 10L, true);

        Assert.assertTrue(backendStatusService.isBackendStatusOk());
    }

    @Test
    public void testBackendFails() throws Exception {
        // Failing a lot sets the backend status to not ok
        QueryService queryService = mock(QueryService.class);
        doAnswer(invocationOnMock -> {
                ((QueryHandler) invocationOnMock.getArguments()[2]).fail(null);
                return null;
            }).when(queryService).submit(any(), any(), any());

        BackendStatusService backendStatusService = new BackendStatusService(queryService, 1L, 10L, true);

        // Sleep for a reasonable amount of time for the backend to fail a lot
        Thread.sleep(1000);

        Assert.assertFalse(backendStatusService.isBackendStatusOk());
    }

    @Test
    public void testBackendNotEnoughFails() throws Throwable {
        // Doesn't fail enough times to set backend status to not ok
        Answer temp = new Answer() {
                int count = 0;
                @Override
                public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                    if (invocationOnMock == null) {
                        return count;
                    }
                    if (count < 9) {
                        ((QueryHandler) invocationOnMock.getArguments()[2]).fail(null);
                        count++;
                    } else {
                        Thread.sleep(60000);
                    }
                    return null;
                }
            };
        QueryService queryService = mock(QueryService.class);
        doAnswer(temp).when(queryService).submit(any(), any(), any());

        BackendStatusService backendStatusService = new BackendStatusService(queryService, 1L, 10L, true);

        // Sleep for a reasonable amount of time for the backend to fail 9 times
        Thread.sleep(1000);

        Assert.assertEquals(temp.answer(null), 9);
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
