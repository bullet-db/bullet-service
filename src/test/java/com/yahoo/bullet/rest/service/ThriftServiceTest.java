/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.rest.resource.DRPCError;
import com.yahoo.bullet.rest.resource.DRPCResponse;
import com.yahoo.bullet.rest.utils.RandomPool;
import org.apache.storm.Config;
import org.apache.storm.utils.DRPCClient;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Map;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ThriftServiceTest {
    @Test
    public void testDefaults() {
        ThriftService service = new ThriftService("foo");
        Assert.assertFalse(service.getConfig().isEmpty());
        Assert.assertEquals(service.getDrpcServers(), new RandomPool<String>(null));
        Assert.assertEquals(service.getDrpcPort(), 3772);
        Assert.assertEquals(service.getDrpcFunction(), "foo");
    }

    @Test
    public void testBadConfig() {
        ThriftService service = new ThriftService("foo");
        Map config = service.getConfig();
        // Change retry to 10 ms
        config.put(Config.STORM_NIMBUS_RETRY_TIMES, 2);
        config.put(Config.STORM_NIMBUS_RETRY_INTERVAL, 10);
        DRPCResponse response = service.invoke("foo");
        Assert.assertTrue(response.hasError());
        Assert.assertEquals(response.getError(), DRPCError.CANNOT_REACH_DRPC);
    }

    @Test
    public void testRunThrough() throws  Exception {
        ThriftService spied = spy(new ThriftService("foo"));
        DRPCClient mocked = mock(DRPCClient.class);
        when(mocked.execute(anyString(), anyString())).thenReturn("response").thenReturn("and so on");
        doReturn(mocked).when(spied).getClient(anyMap(), anyString(), anyInt());

        DRPCResponse first = spied.invoke("foo");
        Assert.assertEquals(first.getContent(), "response");
        Assert.assertNull(first.getError());

        DRPCResponse second = spied.invoke("bar");
        Assert.assertEquals(second.getContent(), "and so on");
        Assert.assertNull(second.getError());

        DRPCResponse third = spied.invoke("baz");
        Assert.assertEquals(third.getContent(), "and so on");
        Assert.assertNull(third.getError());
    }
}
