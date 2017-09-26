/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.query;

import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.rest.query.HTTPQueryHandler;
import com.yahoo.bullet.rest.query.QueryError;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import java.util.UUID;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class HTTPQueryHandlerTest {
    @Test
    public void testSendCompletesAsyncResponseWithContent() {
        AsyncResponse asyncResponse = Mockito.mock(AsyncResponse.class);
        ArgumentCaptor<Response> response = ArgumentCaptor.forClass(Response.class);
        when(asyncResponse.resume(response.capture())).thenReturn(false);
        HTTPQueryHandler queryHandler = new HTTPQueryHandler(asyncResponse);
        String randomContent = UUID.randomUUID().toString();
        queryHandler.send(new PubSubMessage("", randomContent));

        Mockito.verify(asyncResponse).resume(any(Response.class));
        Assert.assertTrue(response.getValue().hasEntity());
        Assert.assertEquals(response.getValue().getStatus(), Response.Status.OK.getStatusCode());
        Assert.assertEquals(response.getValue().getEntity(), randomContent);
    }

    @Test
    public void testSendIsNoopAfterComplete() {
        AsyncResponse asyncResponse = Mockito.mock(AsyncResponse.class);
        ArgumentCaptor<Response> response = ArgumentCaptor.forClass(Response.class);
        when(asyncResponse.resume(response.capture())).thenReturn(false);
        HTTPQueryHandler queryHandler = new HTTPQueryHandler(asyncResponse);
        String randomContent = UUID.randomUUID().toString();
        queryHandler.send(new PubSubMessage("", randomContent));
        queryHandler.complete();
        queryHandler.send(new PubSubMessage("", ""));

        Mockito.verify(asyncResponse).resume(any(Response.class));
    }

    @Test
    public void testFailCompletesAsyncResponseWithError() {
        AsyncResponse asyncResponse = Mockito.mock(AsyncResponse.class);
        ArgumentCaptor<Response> response = ArgumentCaptor.forClass(Response.class);
        when(asyncResponse.resume(response.capture())).thenReturn(false);
        HTTPQueryHandler queryHandler = new HTTPQueryHandler(asyncResponse);
        String randomContent = UUID.randomUUID().toString();
        QueryError cause = new QueryError(randomContent, randomContent);
        queryHandler.fail(cause);

        Assert.assertNotNull(response.getValue());
        Assert.assertEquals(response.getValue().getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        Clip responseEntity = Clip.of(Metadata.of(Error.makeError(cause.getError(), cause.getResolution())));
        Assert.assertEquals(response.getValue().getEntity(), responseEntity.asJSON());
    }

    @Test
    public void testFailIsNoopAfterComplete() {
        AsyncResponse asyncResponse = Mockito.mock(AsyncResponse.class);
        ArgumentCaptor<Response> response = ArgumentCaptor.forClass(Response.class);
        when(asyncResponse.resume(response.capture())).thenReturn(false);
        when(asyncResponse.isDone()).thenReturn(false).thenReturn(true);
        HTTPQueryHandler queryHandler = new HTTPQueryHandler(asyncResponse);
        queryHandler.send(new PubSubMessage("", ""));
        queryHandler.complete();
        queryHandler.fail(QueryError.INVALID_QUERY);

        Mockito.verify(asyncResponse).resume(any(Response.class));
        Assert.assertTrue(response.getValue().hasEntity());
        Assert.assertEquals(response.getValue().getStatus(), Response.Status.OK.getStatusCode());
    }

    @Test
    public void testStaticConstructorInjectsAsyncResponse() {
        AsyncResponse asyncResponse = Mockito.mock(AsyncResponse.class);
        HTTPQueryHandler httpQueryHandler = HTTPQueryHandler.of(asyncResponse);
        Assert.assertEquals(httpQueryHandler.getAsyncResponse(), asyncResponse);
    }

    @Test
    public void testNoCallToQueryHandlerOnAcknowledge() {
        AsyncResponse asyncResponse = Mockito.mock(AsyncResponse.class);
        HTTPQueryHandler httpQueryHandler = HTTPQueryHandler.of(asyncResponse);
        httpQueryHandler.acknowledge();
        Mockito.verifyZeroInteractions(asyncResponse);
    }
}
