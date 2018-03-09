/*
 *  Copyright 2018 Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.rest.model.WebSocketResponse;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
public class WebSocketServiceTest extends AbstractTestNGSpringContextTests {
    @Autowired @InjectMocks
    private WebSocketService webSocketService;
    @Mock
    private SimpMessagingTemplate simpMessagingTemplate;
    @Mock
    private QueryService queryService;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testSendKillSignalWithNonExistingSessionID() {
      webSocketService.getSessionIDMap().clear();
      webSocketService.sendKillSignal("sessionID");

      verify(queryService, never()).submitSignal(any(), any());
    }

    @Test
    public void testSendKillSignalWithExistingSessionID() {
        webSocketService.getSessionIDMap().put("sessionID", "queryID");
        webSocketService.sendKillSignal("sessionID");

        verify(queryService).submitSignal("queryID", Metadata.Signal.KILL);
        Assert.assertFalse(webSocketService.getSessionIDMap().containsKey("sessionID"));
    }

    @Test
    public void testSubmitQuery() {
        webSocketService.getSessionIDMap().clear();
        SimpMessageHeaderAccessor headerAccessor = mock(SimpMessageHeaderAccessor.class);
        when(headerAccessor.getSessionId()).thenReturn("sessionID");
        when(headerAccessor.getMessageHeaders()).thenReturn(null);
        ArgumentCaptor<WebSocketResponse> argument = ArgumentCaptor.forClass(WebSocketResponse.class);

        webSocketService.submitQuery("foo",  headerAccessor);

        verify(simpMessagingTemplate).convertAndSendToUser(
                eq("sessionID"), eq("/response/private"), argument.capture(), eq((MessageHeaders)null));
        String queryID = argument.getValue().getContent();
        verify(queryService).submit(eq(queryID), eq("foo"), any());
        Assert.assertEquals(argument.getValue().getType(), WebSocketResponse.ResponseType.ACK);
        Assert.assertTrue(webSocketService.getSessionIDMap().containsKey("sessionID"));
    }
}
