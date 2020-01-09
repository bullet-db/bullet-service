/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.common;

import com.yahoo.bullet.common.RandomPool;
import com.yahoo.bullet.pubsub.Publisher;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PublisherRandomPoolTest {
    @Test
    public void testClearCloses() throws Exception {
        Publisher publisher = mock(Publisher.class);
        List<Publisher> publishers = Collections.nCopies(5, publisher);
        RandomPool<Publisher> pool = new PublisherRandomPool(publishers);
        pool.clear();
        verify(publisher, times(5)).close();
    }
    
    @Test
    public void testExceptionWhileClosing() throws Exception {
        Publisher publisherA = mock(Publisher.class);
        doThrow(new RuntimeException("Testing")).when(publisherA).close();
        Publisher publisherB = mock(Publisher.class);
        List<Publisher> publishers = Arrays.asList(publisherA, publisherB);
        RandomPool<Publisher> pool = new PublisherRandomPool(publishers);
        pool.clear();
        verify(publisherA).close();
        verify(publisherB).close();
    }
}
