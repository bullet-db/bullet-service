/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.common.metrics.MetricPublisher;
import com.yahoo.bullet.pubsub.IdentityPubSubMessageSerDe;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.PubSubMessageSerDe;
import com.yahoo.bullet.pubsub.PubSubResponder;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.rest.TestHelpers.CustomMetadata;
import com.yahoo.bullet.storage.StorageManager;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.yahoo.bullet.TestHelpers.assertNoMetric;
import static com.yahoo.bullet.rest.TestHelpers.assertMessageEquals;
import static com.yahoo.bullet.rest.TestHelpers.assertMetricsEqual;
import static com.yahoo.bullet.rest.TestHelpers.emptyStorage;
import static com.yahoo.bullet.rest.TestHelpers.failingPublisher;
import static com.yahoo.bullet.rest.TestHelpers.failingStorage;
import static com.yahoo.bullet.rest.TestHelpers.getBQLQuery;
import static com.yahoo.bullet.rest.TestHelpers.getQuery;
import static com.yahoo.bullet.rest.TestHelpers.metadataModifyingPublisher;
import static com.yahoo.bullet.rest.TestHelpers.mockPublisher;
import static com.yahoo.bullet.rest.TestHelpers.mockStorage;
import static com.yahoo.bullet.rest.TestHelpers.unRemovableStorage;
import static com.yahoo.bullet.rest.service.QueryService.QUERY_ADD_FAIL;
import static com.yahoo.bullet.rest.service.QueryService.QUERY_ADD_SUCCESS;
import static com.yahoo.bullet.rest.service.QueryService.QUERY_CLEANUP_FAIL;
import static com.yahoo.bullet.rest.service.QueryService.QUERY_CLEANUP_SUCCESS;
import static com.yahoo.bullet.rest.service.QueryService.QUERY_KILL_FAIL;
import static com.yahoo.bullet.rest.service.QueryService.QUERY_KILL_SUCCESS;
import static com.yahoo.bullet.rest.service.QueryService.QUERY_REMOVE_FAIL;
import static com.yahoo.bullet.rest.service.QueryService.QUERY_REMOVE_SUCCESS;
import static com.yahoo.bullet.rest.service.QueryService.QUERY_RESPONSE_FAIL;
import static com.yahoo.bullet.rest.service.QueryService.QUERY_RESPONSE_SUCCESS;
import static com.yahoo.bullet.rest.service.QueryService.QUERY_RETRIEVE_FAIL;
import static com.yahoo.bullet.rest.service.QueryService.QUERY_RETRIEVE_SUCCESS;
import static com.yahoo.bullet.rest.service.QueryService.QUERY_SUBMIT_FAIL;
import static com.yahoo.bullet.rest.service.QueryService.QUERY_SUBMIT_SUCCESS;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.commons.lang3.tuple.ImmutablePair.of;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

public class QueryServiceTest {
    private Publisher publisher;
    private List<Publisher> publishers;
    private List<Subscriber> subscribers;
    private PubSubResponder responder;
    private List<PubSubResponder> responders;
    private PubSubMessageSerDe serDe;
    private MetricPublisher metricPublisher;

    private static final Query SAMPLE = getQuery();
    private static final String SAMPLE_BQL = getBQLQuery();
    private static final Metadata SAMPLE_METADATA = new Metadata(null, SAMPLE_BQL);

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

        serDe = new IdentityPubSubMessageSerDe(null);
        metricPublisher = mock(MetricPublisher.class);
    }

    @Test
    public void testClose() {
        StorageManager storage = mockStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, serDe, 1, metricPublisher);
        service.close();
        verify(responder).close();
    }

    @Test
    public void testSubmissionPersistsQuery() throws Exception {
        StorageManager storage = mockStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, serDe, 1, metricPublisher);

        PubSubMessage result = service.submit("key", SAMPLE, getBQLQuery()).get();
        PubSubMessage expected = new PubSubMessage("key", SAMPLE, SAMPLE_METADATA);
        assertMessageEquals(result, expected);
        verify(storage).put("key", expected);
        assertMessageSent(publisher, expected);
        assertMetricsEqual(service, 2, of(QUERY_ADD_SUCCESS, 1), of(QUERY_SUBMIT_SUCCESS, 1));
    }

    @Test
    public void testSubmissionPersistsModifiedMessage() throws Exception {
        publisher = metadataModifyingPublisher("testMetadata");
        publishers = singletonList(publisher);
        StorageManager storage = mockStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, serDe, 1, metricPublisher);

        PubSubMessage result = service.submit("key", SAMPLE, SAMPLE_BQL).get();
        // We will answer with a CustomMetadata but the original metadata has nothing anyway
        PubSubMessage expected = new PubSubMessage("key", SAMPLE, SAMPLE_METADATA);
        assertMessageEquals(result, expected);

        ArgumentCaptor<PubSubMessage> messageCaptor = ArgumentCaptor.forClass(PubSubMessage.class);
        verify(publisher).send(messageCaptor.capture());
        PubSubMessage actual = messageCaptor.getValue();
        assertMessageEquals(actual, expected);
        CustomMetadata actualMetadata = (CustomMetadata) actual.getMetadata();
        Assert.assertEquals(actualMetadata.getPayload(), "testMetadata");
        assertMetricsEqual(service, 2, of(QUERY_ADD_SUCCESS, 1), of(QUERY_SUBMIT_SUCCESS, 1));
    }

    @Test
    public void testSubmissionDoesNotPersistIfPublishingFailed() throws Exception {
        PubSubMessage expected = new PubSubMessage("key", SAMPLE, SAMPLE_METADATA);
        publisher = failingPublisher();
        publishers = singletonList(publisher);
        StorageManager storage = mockStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, serDe, 1, metricPublisher);

        PubSubMessage result = service.submit("key", SAMPLE, SAMPLE_BQL).get();
        Assert.assertNull(result);
        assertMessageSent(publisher, expected);
        verifyNoInteractions(storage);
        assertMetricsEqual(service, 1, of(QUERY_SUBMIT_FAIL, 1));
    }

    @Test
    public void testSubmissionIsKilledIfPersistingFailed() throws Exception {
        PubSubMessage expected = new PubSubMessage("key", SAMPLE, SAMPLE_METADATA);
        StorageManager storage = failingStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, serDe, 1, metricPublisher);

        PubSubMessage result = service.submit("key", SAMPLE, SAMPLE_BQL).get();
        Assert.assertNull(result);

        ArgumentCaptor<PubSubMessage> messageCaptor = ArgumentCaptor.forClass(PubSubMessage.class);
        verify(publisher, times(2)).send(messageCaptor.capture());
        List<PubSubMessage> messages = messageCaptor.getAllValues();
        Assert.assertEquals(messages.size(), 2);
        PubSubMessage payload = messages.get(0);
        PubSubMessage kill = messages.get(1);

        assertMessageEquals(payload, expected);
        assertMessageEquals(kill, new PubSubMessage("key", Metadata.Signal.KILL));
        verify(storage).put("key", expected);
        assertMetricsEqual(service, 2, of(QUERY_ADD_FAIL, 1), of(QUERY_SUBMIT_FAIL, 1));
    }

    @Test
    public void testQuerySubmissionDoubleFailureWhenTryingToKillAfterPersistingFailure() throws Exception {
        PubSubMessage expected = new PubSubMessage("key", SAMPLE, SAMPLE_METADATA);
        // Send the message but can't send the kill
        doReturn(expected).doThrow(new RuntimeException("Testing")).when(publisher).send(expected);

        StorageManager storage = emptyStorage();
        CompletableFuture<Boolean> fail = new CompletableFuture<>();
        fail.completeExceptionally(new RuntimeException("Testing"));
        doReturn(fail).when(storage).put(eq("key"), any());

        QueryService service = new QueryService(storage, responders, publishers, subscribers, serDe, 1, metricPublisher);

        PubSubMessage result = service.submit("key", SAMPLE, SAMPLE_BQL).get();
        Assert.assertNull(result);

        assertMessageSent(publisher, expected);
        verify(storage).put("key", expected);
        assertMetricsEqual(service, 1, of(QUERY_SUBMIT_FAIL, 1));
    }

    @Test
    public void testKillingAnExistingQuery() throws Exception {
        StorageManager storage = mockStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, serDe, 1, metricPublisher);

        service.kill("key").get();

        PubSubMessage expected = new PubSubMessage("key", Metadata.Signal.KILL);
        verify(storage).remove("key");
        assertMessageSent(publisher, expected);
        // Removed from storage but message was null so no QUERY_REMOVE_SUCCESS
        assertMetricsEqual(service, 1, of(QUERY_KILL_SUCCESS, 1));
    }

    @Test
    public void testKillingAnExistingQueryRemovesFromStorage() throws Exception {
        StorageManager storage = mockStorage();
        doReturn(completedFuture(new PubSubMessage())).when(storage).remove("key");
        QueryService service = new QueryService(storage, responders, publishers, subscribers, serDe, 1, metricPublisher);

        service.kill("key").get();

        PubSubMessage expected = new PubSubMessage("key", Metadata.Signal.KILL);
        verify(storage).remove("key");
        assertMessageSent(publisher, expected);
        assertMetricsEqual(service, 2, of(QUERY_REMOVE_SUCCESS, 1), of(QUERY_KILL_SUCCESS, 1));
    }

    @Test
    public void testSendingASignal() throws Exception {
        StorageManager storage = mockStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, serDe, 1, metricPublisher);

        service.send("key", Metadata.Signal.KILL).get();
        PubSubMessage expected = new PubSubMessage("key", Metadata.Signal.KILL);
        verifyNoInteractions(storage);
        assertMessageSent(publisher, expected);
        assertNoMetric(service.getMetricCollector().extractMetrics());
    }

    @Test
    public void testSendingAPubSubMessage() throws Exception {
        StorageManager storage = mockStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, serDe, 1, metricPublisher);

        PubSubMessage expected = new PubSubMessage("key", "test", new Metadata(Metadata.Signal.KILL, new HashMap<>()));
        service.send(expected).get();
        verifyNoInteractions(storage);
        assertMessageSent(publisher, expected);
        assertNoMetric(service.getMetricCollector().extractMetrics());
    }

    @Test
    public void testFailingToRemoveFromStorageStillKillsAQuery() throws Exception {
        StorageManager storage = unRemovableStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, serDe, 1, metricPublisher);

        service.kill("key").get();

        PubSubMessage expected = new PubSubMessage("key", Metadata.Signal.KILL);
        verify(storage).remove("key");
        assertMessageSent(publisher, expected);
        assertMetricsEqual(service, 2, of(QUERY_REMOVE_FAIL, 1), of(QUERY_KILL_SUCCESS, 1));
    }

    @Test
    public void testFailingToSendAKillSignalIsIgnored() throws Exception {
        doThrow(new RuntimeException("Testing")).when(publisher).send(any());
        StorageManager storage = mockStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, serDe, 1, metricPublisher);

        service.kill("key").get();

        PubSubMessage expected = new PubSubMessage("key", Metadata.Signal.KILL);
        verify(storage).remove("key");
        assertMessageSent(publisher, expected);
        // Removed from storage but message was null so no QUERY_REMOVE_SUCCESS
        assertMetricsEqual(service, 1, of(QUERY_KILL_FAIL, 1));
    }

    @Test
    public void testFailingToSendAKillSignalIsIgnoredAndQueryIsRemoved() throws Exception {
        doThrow(new RuntimeException("Testing")).when(publisher).send(any());
        StorageManager storage = mockStorage();
        doReturn(completedFuture(new PubSubMessage())).when(storage).remove("key");
        QueryService service = new QueryService(storage, responders, publishers, subscribers, serDe, 1, metricPublisher);

        service.kill("key").get();

        PubSubMessage expected = new PubSubMessage("key", Metadata.Signal.KILL);
        verify(storage).remove("key");
        assertMessageSent(publisher, expected);
        assertMetricsEqual(service, 2, of(QUERY_REMOVE_SUCCESS, 1), of(QUERY_KILL_FAIL, 1));
    }

    @Test
    public void testRespondingToADoneSignal() {
        StorageManager storage = mockStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, serDe, 1, metricPublisher);

        PubSubMessage expected = new PubSubMessage("key", Metadata.Signal.COMPLETE);
        service.respond("key", expected);
        verify(storage).remove("key");
        assertMessageResponded(responder, expected);
        // Removed from storage but message was null so no QUERY_CLEANUP_SUCCESS
        assertMetricsEqual(service, 1, of(QUERY_RESPONSE_SUCCESS, 1));
    }

    @Test
    public void testRespondingToADoneSignalCleansUpExistingQuery() {
        StorageManager storage = mockStorage();
        doReturn(completedFuture(new PubSubMessage())).when(storage).remove("key");
        QueryService service = new QueryService(storage, responders, publishers, subscribers, serDe, 1, metricPublisher);

        PubSubMessage expected = new PubSubMessage("key", Metadata.Signal.COMPLETE);
        service.respond("key", expected);
        verify(storage).remove("key");
        assertMessageResponded(responder, expected);
        assertMetricsEqual(service, 2, of(QUERY_CLEANUP_SUCCESS, 1), of(QUERY_RESPONSE_SUCCESS, 1));
    }

    @Test
    public void testErrorWhileRemovingStillResponds() {
        StorageManager storage = unRemovableStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, serDe, 1, metricPublisher);

        PubSubMessage expected = new PubSubMessage("key", Metadata.Signal.COMPLETE);
        service.respond("key", expected);
        assertMessageResponded(responder, expected);
        assertMetricsEqual(service, 2, of(QUERY_CLEANUP_FAIL, 1), of(QUERY_RESPONSE_SUCCESS, 1));
    }

    @Test
    public void testErrorWhileResponding() {
        StorageManager storage = emptyStorage();
        PubSubResponder mockResponderA = mock(PubSubResponder.class);
        PubSubResponder mockResponderB = mock(PubSubResponder.class);
        PubSubResponder mockResponderC = mock(PubSubResponder.class);
        doThrow(new RuntimeException("Testing")).when(mockResponderB).respond(anyString(), any());
        responders = Arrays.asList(mockResponderA, mockResponderB, mockResponderC);

        QueryService service = new QueryService(storage, responders, publishers, subscribers, serDe, 1, metricPublisher);

        PubSubMessage expected = new PubSubMessage("key", "test");
        service.respond("key", expected);
        assertMessageResponded(mockResponderA, expected);
        assertMessageResponded(mockResponderC, expected);
        assertMetricsEqual(service, 2, of(QUERY_RESPONSE_FAIL, 1), of(QUERY_RESPONSE_SUCCESS, 2));
    }

    @Test
    public void testRespondingToAnything() {
        StorageManager storage = emptyStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, serDe, 1, metricPublisher);

        PubSubMessage expected = new PubSubMessage("key", "test");
        service.respond("key", expected);
        assertMessageResponded(responder, expected);
        assertMetricsEqual(service, 1, of(QUERY_RESPONSE_SUCCESS, 1));
    }

    @Test
    public void testRetrievingASentQuery() throws Exception {
        StorageManager storage = mockStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, serDe, 1, metricPublisher);

        PubSubMessage result = service.submit("key", SAMPLE, getBQLQuery()).get();
        PubSubMessage expected = new PubSubMessage("key", SAMPLE, SAMPLE_METADATA);
        assertMessageEquals(result, expected);
        verify(storage).put("key", expected);
        assertMessageSent(publisher, expected);

        PubSubMessage retrieval = service.get("key").get();
        verify(storage).get("key");
        Assert.assertNull(retrieval);
        // Retrieved from storage but message was null so no QUERY_RETRIEVE_SUCCESS
        assertMetricsEqual(service, 2, of(QUERY_SUBMIT_SUCCESS, 1), of(QUERY_ADD_SUCCESS, 1));
    }

    @Test
    public void testRetrievingASentQueryThatIsStored() throws Exception {
        PubSubMessage expected = new PubSubMessage("key", SAMPLE, SAMPLE_METADATA);

        StorageManager storage = mockStorage();
        doReturn(completedFuture(expected)).when(storage).get("key");
        QueryService service = new QueryService(storage, responders, publishers, subscribers, serDe, 1, metricPublisher);

        PubSubMessage result = service.submit("key", SAMPLE, getBQLQuery()).get();
        assertMessageEquals(result, expected);
        verify(storage).put("key", expected);
        assertMessageSent(publisher, expected);

        PubSubMessage retrieval = service.get("key").get();
        verify(storage).get("key");
        Assert.assertEquals(retrieval.getId(), "key");
        assertMetricsEqual(service, 3, of(QUERY_SUBMIT_SUCCESS, 1), of(QUERY_ADD_SUCCESS, 1), of(QUERY_RETRIEVE_SUCCESS, 1));
    }

    @Test
    public void testErrorWhileRetrievingASentQuery() throws Exception {
        StorageManager storage = unRemovableStorage();
        QueryService service = new QueryService(storage, responders, publishers, subscribers, serDe, 1, metricPublisher);

        PubSubMessage result = service.submit("key", SAMPLE, getBQLQuery()).get();
        PubSubMessage expected = new PubSubMessage("key", SAMPLE, SAMPLE_METADATA);
        assertMessageEquals(result, expected);
        verify(storage).put("key", expected);
        assertMessageSent(publisher, expected);

        PubSubMessage retrieval = service.get("key").get();
        verify(storage).get("key");
        Assert.assertNull(retrieval);
        assertMetricsEqual(service, 3, of(QUERY_SUBMIT_SUCCESS, 1), of(QUERY_ADD_SUCCESS, 1), of(QUERY_RETRIEVE_FAIL, 1));
    }
}
