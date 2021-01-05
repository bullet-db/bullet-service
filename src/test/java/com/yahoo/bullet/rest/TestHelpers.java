/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest;

import com.yahoo.bullet.bql.BulletQueryBuilder;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.query.Window;
import com.yahoo.bullet.query.aggregations.AggregationType;
import com.yahoo.bullet.query.aggregations.Raw;
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
    private static final String INVALID_QUERY_BQL = "SELECT * FROM STREAM(1000, TIME) WHERE 1 + 'foo';";
    private static final String SAMPLE_QUERY_BQL = "SELECT * FROM STREAM(1000, TIME) LIMIT 1";

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

    private static StorageManager<Serializable> storage(boolean canStore) {
        StorageManager<Serializable> storage = mock(StorageManager.class);
        doReturn(CompletableFuture.completedFuture(canStore)).when(storage).put(anyString(), any(Serializable.class));
        return storage;
    }

    public static StorageManager<Serializable> emptyStorage() {
        return storage(true);
    }

    public static StorageManager<Serializable> failingStorage() {
        return storage(false);
    }

    public static StorageManager<Serializable> unRemovableStorage() {
        StorageManager<Serializable> storage = emptyStorage();
        CompletableFuture<Serializable> mock = new CompletableFuture<>();
        mock.completeExceptionally(new RuntimeException("Testing"));
        doReturn(mock).when(storage).remove("key");
        return storage;
    }

    public static StorageManager<Serializable> mockStorage() {
        StorageManager<Serializable> service = emptyStorage();
        doReturn(CompletableFuture.completedFuture(null)).when(service).get(anyString());
        doReturn(CompletableFuture.completedFuture(null)).when(service).remove(anyString());
        return service;
    }

    public static StorageManager<Serializable> mockStorage(Serializable data) {
        StorageManager<Serializable> service = emptyStorage();
        doReturn(CompletableFuture.completedFuture(data)).when(service).get(anyString());
        doReturn(CompletableFuture.completedFuture(data)).when(service).remove(anyString());
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

    public static String getInvalidBQLQuery() {
        return INVALID_QUERY_BQL;
    }

    public static String getBQLQuery() {
        return SAMPLE_QUERY_BQL;
    }

    public static Query getQuery() {
        return new Query(new Projection(), null, new Raw(1), null, new Window(), 1000L);
    }

    public static Query getQueryWithWindow(Window window) {
        return new Query(new Projection(), null, new Raw(1), null, window, 1000L);
    }

    public static void assertEqualsQuery(Query actual) {
        Assert.assertEquals(actual.getAggregation().getType(), AggregationType.RAW);
        Assert.assertEquals(actual.getAggregation().getSize(), (Integer) 1);
        Assert.assertEquals(actual.getDuration(), (Long) 1000L);
        Assert.assertNull(actual.getFilter());
        Assert.assertNull(actual.getProjection().getFields());
        Assert.assertEquals(actual.getProjection().getType(), Projection.Type.PASS_THROUGH);
        Assert.assertNull(actual.getWindow().getType());
        Assert.assertNull(actual.getPostAggregations());
    }

    public static void assertEqualsBql(String actual) {
        Assert.assertEquals(actual, SAMPLE_QUERY_BQL);
    }
}
