/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.PubSubResponder;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import com.yahoo.bullet.rest.TestHelpers.CustomMetadata;
import com.yahoo.bullet.storage.StorageManager;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.yahoo.bullet.rest.TestHelpers.assertMessageEquals;
import static com.yahoo.bullet.rest.TestHelpers.emptyStorage;
import static com.yahoo.bullet.rest.TestHelpers.failingPublisher;
import static com.yahoo.bullet.rest.TestHelpers.failingStorage;
import static com.yahoo.bullet.rest.TestHelpers.metadataModifyingPublisher;
import static com.yahoo.bullet.rest.TestHelpers.mockPublisher;
import static com.yahoo.bullet.rest.TestHelpers.mockStorage;
import static com.yahoo.bullet.rest.TestHelpers.unRemovableStorage;
import static java.util.Collections.singletonList;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class QueryServiceTest {
    private Publisher publisher;
    private List<Publisher> publishers;
    private List<Subscriber> subscribers;
    private PubSubResponder responder;
    private List<PubSubResponder> responders;

    private void assertMessageResponded(PubSubResponder responderMock, PubSubMessage expected) {
        ArgumentCaptor<PubSubMessage> messageCaptor = ArgumentCaptor.forClass(PubSubMessage.class);
        verify(responderMock).respond(eq(expected.getId()), messageCaptor.capture());
        assertMessageEquals(messageCaptor.getValue(), expected);
    }

    private void assertMessageSent(Publisher publisherMock, PubSubMessage expected) throws Exception {
        ArgumentCaptor<PubSubMessage> messageCaptor = ArgumentCaptor.forClass(PubSubMessage.class);
        verify(publisherMock).send(messageCaptor.capture());
        assertMessageEquals(messageCaptor.getValue(), expected);
    }

    @BeforeMethod
    private void setup() throws Exception {
        publisher = mockPublisher();
        publishers = singletonList(publisher);

        Subscriber subscriber = mock(Subscriber.class);
        doReturn(null).when(subscriber).receive();
        subscribers = singletonList(subscriber);

        responder = mock(PubSubResponder.class);
        responders = singletonList(responder);
    }

    @Test
    public void testClose() {
        StorageManager storage = mockStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, 1);
        service.close();
        verify(responder).close();
        verify(storage).close();
    }

    @Test
    public void testSubmissionPersistsQuery() throws Exception {
        StorageManager storage = mockStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, 1);

        PubSubMessage result = service.submit("key", "{}").get();
        PubSubMessage expected = new PubSubMessage("key", "{}");
        assertMessageEquals(result, expected);
        verify(storage).putObject("key", expected);
        assertMessageSent(publisher, expected);
    }

    @Test
    public void testSubmissionPersistsModifiedMessage() throws Exception {
        publisher = metadataModifyingPublisher("testMetadata");
        publishers = singletonList(publisher);
        StorageManager storage = mockStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, 1);

        PubSubMessage result = service.submit("key", "{}").get();
        // We will answer with a CustomMetadata but the original metadata has nothing anyway
        PubSubMessage expected = new PubSubMessage("key", "{}", new Metadata());
        assertMessageEquals(result, expected);

        ArgumentCaptor<PubSubMessage> messageCaptor = ArgumentCaptor.forClass(PubSubMessage.class);
        verify(publisher).send(messageCaptor.capture());
        PubSubMessage actual = messageCaptor.getValue();
        assertMessageEquals(actual, expected);
        CustomMetadata actualMetadata = (CustomMetadata) actual.getMetadata();
        Assert.assertEquals(actualMetadata.getPayload(), "testMetadata");
    }

    @Test
    public void testSubmissionDoesNotPersistIfPublishingFailed() throws Exception {
        PubSubMessage expected = new PubSubMessage("key", "{}");
        publisher = failingPublisher();
        publishers = singletonList(publisher);
        StorageManager storage = mockStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, 1);

        PubSubMessage result = service.submit("key", "{}").get();
        Assert.assertNull(result);
        assertMessageSent(publisher, expected);
        verifyZeroInteractions(storage);
    }

    @Test
    public void testSubmissionIsKilledIfPersistingFailed() throws Exception {
        PubSubMessage expected = new PubSubMessage("key", "{}");
        StorageManager storage = failingStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, 1);

        PubSubMessage result = service.submit("key", "{}").get();
        Assert.assertNull(result);

        ArgumentCaptor<PubSubMessage> messageCaptor = ArgumentCaptor.forClass(PubSubMessage.class);
        verify(publisher, times(2)).send(messageCaptor.capture());
        List<PubSubMessage> messages = messageCaptor.getAllValues();
        Assert.assertEquals(messages.size(), 2);
        PubSubMessage payload = messages.get(0);
        PubSubMessage kill = messages.get(1);

        assertMessageEquals(payload, expected);
        assertMessageEquals(kill, new PubSubMessage("key", Metadata.Signal.KILL));
        verify(storage).putObject("key", expected);
    }

    @Test
    public void testQuerySubmissionDoubleFailureWhenTryingToKillAfterPersistingFailure() throws Exception {
        PubSubMessage expected = new PubSubMessage("key", "{}");
        // Send the message but can't send the kill
        doReturn(expected).doThrow(new RuntimeException("Testing")).when(publisher).send(expected);

        StorageManager storage = emptyStorage();
        CompletableFuture<Boolean> fail = new CompletableFuture<>();
        fail.completeExceptionally(new RuntimeException("Testing"));
        doReturn(fail).when(storage).putObject(eq("key"), any());

        QueryService service = new QueryService(storage, responders, publishers, subscribers, 1);

        PubSubMessage result = service.submit("key", "{}").get();
        Assert.assertNull(result);

        assertMessageSent(publisher, expected);
        verify(storage).putObject("key", expected);
    }

    @Test
    public void testKillingAnExistingQuery() throws Exception {
        StorageManager storage = mockStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, 1);

        service.kill("key").get();

        PubSubMessage expected = new PubSubMessage("key", Metadata.Signal.KILL);
        verify(storage).removeObject("key");
        assertMessageSent(publisher, expected);
    }

    @Test
    public void testSendingASignal() throws Exception {
        StorageManager storage = mockStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, 1);

        service.send("key", Metadata.Signal.KILL).get();
        PubSubMessage expected = new PubSubMessage("key", Metadata.Signal.KILL);
        verifyZeroInteractions(storage);
        assertMessageSent(publisher, expected);
    }

    @Test
    public void testSendingAPubSubMessage() throws Exception {
        StorageManager storage = mockStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, 1);

        PubSubMessage expected = new PubSubMessage("key", "test", new Metadata(Metadata.Signal.KILL, new HashMap<>()));
        service.send(expected).get();
        verifyZeroInteractions(storage);
        assertMessageSent(publisher, expected);
    }

    @Test
    public void testFailingToRemoveFromStorageStillKillsAQuery() throws Exception {
        StorageManager storage = unRemovableStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, 1);

        service.kill("key").get();

        PubSubMessage expected = new PubSubMessage("key", Metadata.Signal.KILL);
        verify(storage).removeObject("key");
        assertMessageSent(publisher, expected);
    }

    @Test
    public void testFailingToSendAKillSignalIsIgnored() throws Exception {
        doThrow(new RuntimeException("Testing")).when(publisher).send(any());
        StorageManager storage = mockStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, 1);

        service.kill("key").get();

        PubSubMessage expected = new PubSubMessage("key", Metadata.Signal.KILL);
        verify(storage).removeObject("key");
        assertMessageSent(publisher, expected);
    }

    @Test
    public void testRespondingToADoneSignal() {
        StorageManager storage = mockStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, 1);

        PubSubMessage expected = new PubSubMessage("key", Metadata.Signal.COMPLETE);
        service.respond("key", expected);
        assertMessageResponded(responder, expected);
    }

    @Test
    public void testErrorWhileRemovingStillResponds() {
        StorageManager storage = unRemovableStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, 1);

        PubSubMessage expected = new PubSubMessage("key", Metadata.Signal.COMPLETE);
        service.respond("key", expected);
        assertMessageResponded(responder, expected);
    }

    @Test
    public void testRespondingToAnything() {
        StorageManager storage = emptyStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, 1);

        PubSubMessage expected = new PubSubMessage("key", "{}");
        service.respond("key", expected);
        assertMessageResponded(responder, expected);
    }
}
