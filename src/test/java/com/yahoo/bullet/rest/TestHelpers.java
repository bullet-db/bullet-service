/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest;

import com.yahoo.bullet.bql.BulletQueryBuilder;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.storage.StorageManager;
import lombok.Getter;
import org.testng.Assert;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class TestHelpers {
    private static final BulletQueryBuilder QUERY_BUILDER = new BulletQueryBuilder(new BulletConfig());
    private static final String SAMPLE_QUERY_BQL = "SELECT * FROM STREAM(1000, TIME) LIMIT 1;";
    private static final Query SAMPLE_QUERY = new Query();
    static {
        SAMPLE_QUERY .setAggregation(new Aggregation(1, Aggregation.Type.RAW));
        SAMPLE_QUERY .setDuration(1000L);
    }

    public static class CustomMetadata extends Metadata {
        private static final long serialVersionUID = 6927372987820050551L;
        @Getter
        private String payload;

        CustomMetadata(String payload, Metadata other) {
            if (other != null) {
                this.setContent(other.getContent());
                this.setSignal(other.getSignal());
            }
            this.payload = payload;
        }
    }

    public static Publisher mockPublisher() throws Exception {
        Publisher publisher = mock(Publisher.class);
        doAnswer(i -> i.getArgumentAt(0, PubSubMessage.class)).when(publisher).send(any());
        return publisher;
    }

    public static Publisher failingPublisher() throws Exception {
        Publisher publisher = mock(Publisher.class);
        doThrow(new RuntimeException("Testing")).when(publisher).send(any());
        return publisher;
    }

    public static Publisher metadataModifyingPublisher(String extraMetadata) throws Exception {
        Publisher publisher = mock(Publisher.class);
        doAnswer(invocation -> {
            PubSubMessage message = invocation.getArgumentAt(0, PubSubMessage.class);
            message.setMetadata(new CustomMetadata(extraMetadata, message.getMetadata()));
            return message;
        }).when(publisher).send(any());
        return publisher;
    }

    private static StorageManager storage(boolean canStore) {
        StorageManager storage = mock(StorageManager.class);
        doReturn(CompletableFuture.completedFuture(canStore)).when(storage).putObject(anyString(), any());
        return storage;
    }

    public static StorageManager emptyStorage() {
        return storage(true);
    }

    public static StorageManager failingStorage() {
        return storage(false);
    }

    public static StorageManager unRemovableStorage() {
        StorageManager storage = emptyStorage();
        CompletableFuture<Serializable> mock = new CompletableFuture<>();
        mock.completeExceptionally(new RuntimeException("Testing"));
        doReturn(mock).when(storage).removeObject("key");
        return storage;
    }

    public static StorageManager mockStorage() {
        StorageManager service = emptyStorage();
        doReturn(CompletableFuture.completedFuture(null)).when(service).getObject(anyString());
        doReturn(CompletableFuture.completedFuture(null)).when(service).removeObject(anyString());
        return service;
    }

    public static StorageManager mockStorage(Serializable data) {
        StorageManager service = emptyStorage();
        doReturn(CompletableFuture.completedFuture(data)).when(service).getObject(anyString());
        doReturn(CompletableFuture.completedFuture(data)).when(service).removeObject(anyString());
        return service;
    }

    public static void assertMessageEquals(PubSubMessage actual, PubSubMessage expected) {
        Assert.assertEquals(actual, expected);
        Assert.assertEquals(actual.getContent(), expected.getContent());
        Metadata actualMetadata = actual.getMetadata();
        Metadata expectedMetadata = expected.getMetadata();
        if (actualMetadata == null || expectedMetadata == null) {
            Assert.assertNull(actualMetadata, "Metadata was null when expected to be not");
            Assert.assertNull(expectedMetadata, "Metadata was not null when expected to be ");
            return;
        }
        Assert.assertEquals(actualMetadata.getSignal(), expectedMetadata.getSignal());
        Assert.assertEquals(actualMetadata.getContent(), expectedMetadata.getContent());
    }

    public static BulletQueryBuilder getQueryBuilder() {
        return QUERY_BUILDER;
    }

    public static String getSampleBQLQuery() {
        return SAMPLE_QUERY_BQL;
    }

    public static Query getSampleQuery() {
        return SAMPLE_QUERY;
    }

    public static void assertEqualsSampleQuery(Query actual) {
        Assert.assertEquals(actual.getAggregation().getType(), Aggregation.Type.RAW);
        Assert.assertEquals(actual.getAggregation().getSize(), (Integer) 1);
        Assert.assertEquals(actual.getDuration(), (Long) 1000L);
        Assert.assertNull(actual.getFilter());
        Assert.assertNull(actual.getProjection());
        Assert.assertNull(actual.getWindow());
        Assert.assertNull(actual.getPostAggregations());
    }
}
