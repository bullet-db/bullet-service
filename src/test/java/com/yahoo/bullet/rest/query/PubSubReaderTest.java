/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.query;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Subscriber;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class PubSubReaderTest {
    private ConcurrentHashMap<String, QueryHandler> requestQueue;
    private String randomID;
    private MockQueryHandler queryHandler;
    private PubSubMessage mockMessage;

    @Getter @NoArgsConstructor
    private class MockQueryHandler extends QueryHandler {
        private CompletableFuture<PubSubMessage> sentMessage = new CompletableFuture<>();
        private CompletableFuture<Boolean> isCompleted = new CompletableFuture<>();

        @Override
        public void send(PubSubMessage message) {
            sentMessage.complete(message);
        }

        @Override
        public void complete() {
            super.complete();
            isCompleted.complete(true);
        }

        @Override
        public void fail(QueryError cause) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void acknowledge() {
            throw new UnsupportedOperationException();
        }
    }

    @NoArgsConstructor @Getter
    private class MockSubscriber implements Subscriber {
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
        public void close() {
            isClosed.complete(true);
        }

        @Override
        public void commit(String s, int i) {
            committedID.complete(s);
        }

        @Override
        public void fail(String s, int i) {
        }
    }

    private class MockFailingSubscriber extends MockSubscriber {
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

    @BeforeMethod
    public void setup() {
        requestQueue = new ConcurrentHashMap<>();
        randomID = UUID.randomUUID().toString();
        queryHandler = new MockQueryHandler();
        mockMessage = new PubSubMessage(randomID, "foo");
    }

    @Test(timeOut = 10000)
    public void testQueryCompletedOnReceive() throws Exception {
        Subscriber subscriber = new MockSubscriber(mockMessage);
        requestQueue.put(randomID, queryHandler);
        PubSubReader reader = new PubSubReader(subscriber, requestQueue, 1);
        Assert.assertEquals(queryHandler.getSentMessage().get(), mockMessage);
        reader.close();
    }

    @Test(timeOut = 10000)
    public void testQueryCompletedOnReceiveFailMessage() throws Exception {
        PubSubMessage failMessage = new PubSubMessage("failID", "", Metadata.Signal.FAIL);
        Subscriber subscriber = new MockSubscriber(failMessage);
        requestQueue.put("failID", queryHandler);
        PubSubReader reader = new PubSubReader(subscriber, requestQueue, 1);
        Assert.assertTrue(queryHandler.getIsCompleted().get());
        Assert.assertEquals(queryHandler.getSentMessage().get(), failMessage);
        reader.close();
    }

    @Test(timeOut = 10000)
    public void testSubscriberClosedOnClose() throws Exception {
        MockSubscriber subscriber = new MockSubscriber();
        PubSubReader reader = new PubSubReader(subscriber, requestQueue, 1);
        reader.close();
        Assert.assertTrue(subscriber.getIsClosed().get());
    }

    @Test(timeOut = 10000)
    public void testSubscriberClosedOnError() throws Exception {
        // When a runtime exception occurs, the reader shuts down and close is called on the Subscriber.
        MockFailingSubscriber subscriber = new MockFailingSubscriber();
        new PubSubReader(subscriber, requestQueue, 1);

        Assert.assertTrue(subscriber.getIsClosed().get());
    }

    @Test(timeOut = 10000)
    public void testEmptyReceiveIgnored() throws Exception {
        Subscriber subscriber = new MockSubscriber(null, mockMessage);
        requestQueue.put(randomID, queryHandler);
        new PubSubReader(subscriber, requestQueue, 1);

        Assert.assertEquals(queryHandler.getSentMessage().get(), mockMessage);
    }

    @Test(timeOut = 10000)
    public void testMessageIgnoredWhenQueryHandlerComplete() throws Exception {
        PubSubMessage completedMessage = new PubSubMessage("completedID", "");
        MockQueryHandler completedQueryHandler = new MockQueryHandler();
        completedQueryHandler.complete();
        Subscriber subscriber = new MockSubscriber(completedMessage, mockMessage);
        requestQueue.put(randomID, queryHandler);
        requestQueue.put("completedID", completedQueryHandler);
        new PubSubReader(subscriber, requestQueue, 1);

        // Waits for message after completedMessage to get processed and checks that completedQueryHandler send was not invoked.
        Assert.assertEquals(queryHandler.getSentMessage().get(), mockMessage);
        Assert.assertFalse(completedQueryHandler.getSentMessage().isDone());
    }

    @Test(timeOut = 10000)
    public void testAbsentQueryHandlerMessageAcked() throws Exception {
        MockSubscriber mockSubscriber = new MockSubscriber(mockMessage);
        new PubSubReader(mockSubscriber, requestQueue, 1);

        Assert.assertEquals(mockSubscriber.getCommittedID().get(), randomID);
    }
}
