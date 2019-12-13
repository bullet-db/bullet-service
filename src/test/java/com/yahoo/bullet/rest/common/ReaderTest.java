/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.common;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.PubSubResponder;
import com.yahoo.bullet.pubsub.Subscriber;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class ReaderTest {
    private PubSubMessage mockMessage;
    private MockResponder responder;

    @Getter
    private static class MockResponder implements PubSubResponder {
        private CompletableFuture<PubSubMessage> sentMessage = new CompletableFuture<>();
        private CompletableFuture<Boolean> isCompleted = new CompletableFuture<>();

        @Override
        public void respond(String id, PubSubMessage message) {
            sentMessage.complete(message);
        }
    }

    @NoArgsConstructor @Getter
    private static class MockSubscriber implements Subscriber {
        protected CompletableFuture<Boolean> isClosed = new CompletableFuture<>();
        private LinkedList<PubSubMessage> messageList = null;
        private CompletableFuture<String> committedID = new CompletableFuture<>();

        MockSubscriber(PubSubMessage... messages) {
            messageList = new LinkedList<>();
            messageList.addAll(Arrays.asList(messages));
        }

        @Override
        public PubSubMessage receive() throws PubSubException {
            return messageList == null || messageList.isEmpty() ? null : messageList.remove(0);
        }

        @Override
        public void close() throws Exception {
            isClosed.complete(true);
        }

        @Override
        public void commit(String s) {
            committedID.complete(s);
        }

        @Override
        public void fail(String s) {
        }
    }

    private static class MockFailingSubscriber extends MockSubscriber {
        private boolean receiveWasCalled = false;

        @Override
        public PubSubMessage receive() throws PubSubException {
            receiveWasCalled = true;
            throw new PubSubException("bar");
        }

        @Override
        public void close() {
            isClosed.complete(receiveWasCalled);
        }
    }

    @Getter
    private static class MockUnCloseableSubscriber extends MockSubscriber {
        private CompletableFuture<Boolean> didError = new CompletableFuture<>();

        MockUnCloseableSubscriber(PubSubMessage... messages) {
            super(messages);
        }

        @Override
        public void close() throws Exception {
            didError.complete(true);
            throw new IOException("Test");
        }
    }

    @BeforeMethod
    public void setup() {
        String randomID = UUID.randomUUID().toString();
        responder = new MockResponder();
        mockMessage = new PubSubMessage(randomID, "foo");
    }

    @Test(timeOut = 10000)
    public void testMessageRespondedTo() throws Exception {
        Subscriber subscriber = new MockSubscriber(mockMessage);
        Reader reader = new Reader(subscriber, responder, 1);
        Assert.assertEquals(responder.getSentMessage().get(), mockMessage);
        reader.close();
    }

    @Test(timeOut = 10000)
    public void testSubscriberClosedOnClose() throws Exception {
        MockSubscriber subscriber = new MockSubscriber();
        Reader reader = new Reader(subscriber, responder, 1);
        reader.close();
        Assert.assertTrue(subscriber.getIsClosed().get());
    }

    @Test(timeOut = 10000)
    public void testSubscriberClosedOnError() throws Exception {
        // When a runtime exception occurs, the reader shuts down and close is called on the Subscriber.
        MockFailingSubscriber subscriber = new MockFailingSubscriber();
        new Reader(subscriber, responder, 1);

        Assert.assertTrue(subscriber.getIsClosed().get());
    }

    @Test(timeOut = 10000)
    public void testEmptyReceiveIgnored() throws Exception {
        Subscriber subscriber = new MockSubscriber(null, mockMessage);
        new Reader(subscriber, responder, 1);

        Assert.assertEquals(responder.getSentMessage().get(), mockMessage);
    }

    @Test(timeOut = 10000)
    public void testExceptionOnSubscriberClose() throws Exception {
        MockUnCloseableSubscriber subscriber = new MockUnCloseableSubscriber(mockMessage);
        Reader reader = new Reader(subscriber, responder, 1);
        Assert.assertEquals(responder.getSentMessage().get(), mockMessage);
        reader.close();
        Assert.assertTrue(subscriber.getDidError().get());
    }
}
