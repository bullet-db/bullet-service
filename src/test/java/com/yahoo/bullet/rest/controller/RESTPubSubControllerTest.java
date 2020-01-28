/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.rest.service.RESTPubSubService;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletResponse;

import static com.yahoo.bullet.TestHelpers.assertJSONEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RESTPubSubControllerTest {
    private RESTPubSubController controller;

    @BeforeMethod
    public void setup() {
        controller = new RESTPubSubController(new RESTPubSubService());
    }

    @Test
    public void testPostAndGetQuery() {
        HttpServletResponse response = mock(HttpServletResponse.class);

        controller.postQuery("{'id': '88', 'sequence': -1, 'content': 'foo', 'metadata': null}");
        String result = controller.getQuery(response);
        assertJSONEquals(result, "{'id': '88', 'sequence': -1, 'content': 'foo', 'metadata': null}");
        verify(response, Mockito.never()).setStatus(Mockito.anyInt());

        controller.getQuery(response);
        verify(response).setStatus(204);
    }

    @Test
    public void testPostAndGetResult() {
        HttpServletResponse response = mock(HttpServletResponse.class);

        controller.postResult("{'id': '88', 'sequence': -1, 'content': 'foo', 'metadata': null}");
        String result = controller.getResult(response);
        assertJSONEquals(result, "{'id': '88', 'sequence': -1, 'content': 'foo', 'metadata': null}");
        verify(response, Mockito.never()).setStatus(Mockito.anyInt());

        controller.getResult(response);
        verify(response).setStatus(204);
    }
}
