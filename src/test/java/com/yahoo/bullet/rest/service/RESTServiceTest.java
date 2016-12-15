/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.rest.resource.DRPCError;
import com.yahoo.bullet.rest.resource.DRPCResponse;
import com.yahoo.bullet.rest.utils.RandomPool;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.Stubber;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static java.util.Collections.singletonList;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ContextConfiguration(locations = "/TestApplicationContext.xml")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class RESTServiceTest extends AbstractTestNGSpringContextTests {
    @Autowired
    private RESTService drpcService;

    private Stubber add(Stubber ongoingStub, Object object) {
        Answer next = invocation -> {
            if (object instanceof Exception) {
                throw (Throwable) object;
            } else {
                Object[] args = invocation.getArguments();
                // The second argument is the url. Add that to the object
                return new DRPCResponse(args[1] + " : " + object);
            }
        };
        return ongoingStub == null ? doAnswer(next) : ongoingStub.doAnswer(next);
    }

    /**
     * A function for testing that will return a mocked Client that will return the provided Response object.
     *
     * @param response The response (usually mocked) Response that the returned Client will return
     * @return A Client which will return the provided Response
     */
    private Client makeClient(Response response) {
        Invocation.Builder builder = mock(Invocation.Builder.class);
        when(builder.post(any(Entity.class))).thenReturn(response);

        WebTarget target = mock(WebTarget.class);
        when(target.request(MediaType.TEXT_PLAIN)).thenReturn(builder);

        Client client = mock(Client.class);
        when(client.target(anyString())).thenReturn(target);

        return client;
    }

    private Response makeResponse(String entity, Response.Status status) {
        Response response = mock(Response.class);
        when(response.getStatusInfo()).thenReturn(status);
        when(response.readEntity(String.class)).thenReturn(entity);
        return response;
    }

    private RESTService makeResponses(Object... responses) {
        RESTService spied = Mockito.spy(drpcService);
        Stubber stub = null;
        for (Object response : responses) {
            stub = add(stub, response);
        }
        stub.doThrow(new ProcessingException("")).when(spied).makeRequest(any(Client.class), anyString(), anyString());
        return spied;
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testBadInitialization() {
        new RESTService(null, null, null);
    }

    @Test
    public void testEmptyBehavior() {
        RESTService service = new RESTService("", "", "");
        Assert.assertEquals(service.getUrls(), new RandomPool(singletonList("http://:/")));
        Assert.assertEquals(service.getConnectTimeout(), 0);
        Assert.assertEquals(service.getRetryLimit(), 0);
    }

    @Test
    public void testRequestToRandomURL() {
        Assert.assertNotNull(new RESTService("", "", "").makeRequest(ClientBuilder.newClient(), "http://www.yahoo.com", "foo"));
    }

    @Test
    public void testMakeRequestSuccess() {
        Response response = makeResponse("good", Response.Status.OK);
        Client client = makeClient(response);

        DRPCResponse drpcResponse = drpcService.makeRequest(client, "ignored", "ignored");
        Assert.assertFalse(drpcResponse.hasError());
        Assert.assertEquals(drpcResponse.getContent(), "good");
    }

    @Test
    public void testConfiguredBehavior() {
        Assert.assertEquals(drpcService.getUrls(), new RandomPool(singletonList("http://foo.bar.com:4080/drpc/tracer")));
        Assert.assertEquals(drpcService.getConnectTimeout(), 100);
        Assert.assertEquals(drpcService.getRetryLimit(), 2);
    }

    @Test
    public void testConfiguredInvocation() {
        DRPCResponse response = drpcService.invoke("ignored");
        Assert.assertTrue(response.hasError());
        Assert.assertEquals(response.getError(), DRPCError.RETRY_LIMIT_EXCEEDED);
    }

    @Test
    public void testRetry() {
        RESTService service = makeResponses(new ProcessingException("1"), "2");
        DRPCResponse response = service.invoke("ignored");
        Assert.assertEquals(response.getContent(), "http://foo.bar.com:4080/drpc/tracer : 2");
        Assert.assertNull(response.getError());
    }

    @Test
    public void testFullFailure() {
        RESTService service = makeResponses(new ProcessingException("1"), new ProcessingException("2"),
                                            new ProcessingException("3"), new ProcessingException("4"));
        DRPCResponse response = service.invoke("ignored");
        Assert.assertTrue(response.hasError());
        Assert.assertEquals(response.getError(), DRPCError.RETRY_LIMIT_EXCEEDED);
    }
}
