/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.PubSubResponder;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import com.yahoo.bullet.rest.common.PublisherRandomPool;
import com.yahoo.bullet.rest.common.Reader;
import com.yahoo.bullet.rest.common.Utils;
import com.yahoo.bullet.storage.StorageManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Slf4j
@Service
public class QueryService extends PubSubResponder {
    private StorageManager storage;
    private List<PubSubResponder> responders;
    private PublisherRandomPool publishers;
    private List<Reader> readers;

    private static final CompletableFuture<PubSubMessage> NONE = CompletableFuture.completedFuture(null);
    private static final CompletableFuture<Boolean> SUCCESS = CompletableFuture.completedFuture(true);
    private static final CompletableFuture<Boolean> FAIL = CompletableFuture.completedFuture(false);

    /**
     * Constructor that takes various necessary components.
     *
     * @param storageManager The non-null {@link StorageManager} to use.
     * @param responders The non-empty {@link List} of {@link PubSubResponder} to use.
     * @param publishers The non-empty {@link List} of {@link Publisher} to use.
     * @param subscribers The non-empty {@link List} of {@link Subscriber} to use.
     * @param sleep The time to sleep between checking for messages from the pubsub.
     */
    @Autowired
    public QueryService(StorageManager storageManager, List<PubSubResponder> responders, List<Publisher> publishers,
                        List<Subscriber> subscribers, @Value("${bullet.pubsub.sleep-ms}") int sleep) {
        super(null);
        Objects.requireNonNull(storageManager);
        Utils.checkNotEmpty(responders);
        Utils.checkNotEmpty(publishers);
        Utils.checkNotEmpty(subscribers);
        this.storage = storageManager;
        this.responders = responders;
        this.publishers = new PublisherRandomPool(publishers);
        this.readers = subscribers.stream().map(x -> new Reader(x, this, sleep)).collect(Collectors.toList());
        this.readers.forEach(Reader::start);
    }

    /**
     * Submit a query to Bullet and store it in the storage. Unless the publishing succeeds, the query is not stored.
     *
     * @param id The query ID of the query.
     * @param query The query to send.
     * @return A {@link CompletableFuture} that resolves to the sent {@link PubSubMessage} or null if it could not be sent.
     */
    public CompletableFuture<PubSubMessage> submit(String id, Query query) {
        log.debug("Submitting query {}", id);
        PubSubMessage message = new PubSubMessage(id, SerializerDeserializer.toBytes(query));
        // Publish then store. Publishing might change the message. Store the sent result
        return publish(message).thenComposeAsync(sent -> store(id, sent))
                               .thenApply(sent -> onSubmit(id, sent))
                               .exceptionally(e -> onSubmitFail(e, id));
    }

    /**
     * Submits a {@link Metadata.Signal#KILL} signal to Bullet for the given query ID and removes the query.
     *
     * @param id The query ID to submit the kill signal for.
     * @return A {@link CompletableFuture} that resolves when the kill was finished.
     */
    public CompletableFuture<Void> kill(String id) {
        log.debug("Removing metadata for query {} and killing it", id);
        CompletableFuture<PubSubMessage> removed = storage.removeObject(id);
        return removed.thenAccept(QueryService::onStoredMessageRemove)
                      .exceptionally(e -> onStoredMessageRemoveFail(e, id))
                      .thenAccept(u -> killQuery(id));
    }

    /**
     * Respond to a {@link PubSubMessage}.
     *
     * @param id The id of the query.
     * @param response The {@link PubSubMessage} response.
     */
    public void respond(String id, PubSubMessage response) {
        log.debug("Received response {} for {}", id, response);
        if (Utils.isDone(response)) {
            CompletableFuture<PubSubMessage> removed = storage.removeObject(id);
            removed.thenAccept(QueryService::onStoredMessageRemove)
                   .exceptionally(e -> onRespondFail(e, id, response));
        }
        responders.forEach(responder -> responder.respond(id, response));
    }

    /**
     * Sends a {@link Metadata.Signal} to Bullet without storing it.
     *
     * @param id The non-null ID of the message to send this signal in.
     * @param signal The non-null {@link Metadata.Signal} to send.
     * @return A {@link CompletableFuture} that resolves to the sent {@link PubSubMessage} or null if it could not be sent.
     */
    public CompletableFuture<PubSubMessage> send(String id, Metadata.Signal signal) {
        Objects.requireNonNull(signal);
        return publish(new PubSubMessage(id, signal));
    }

    /**
     * Sends a {@link PubSubMessage} to Bullet without storing it. This can be used to send signals or anything else.
     *
     * @param message The non-null {@link PubSubMessage} to send.
     * @return A {@link CompletableFuture} that resolves to the sent {@link PubSubMessage} or null if it could not be sent.
     */
    public CompletableFuture<PubSubMessage> send(PubSubMessage message) {
        Objects.requireNonNull(message);
        return publish(message);
    }

    /**
     * Stop all service threads and clear pending requests.
     */
    @PreDestroy
    @Override
    public void close() {
        readers.forEach(Reader::close);
        responders.forEach(PubSubResponder::close);
        storage.close();
        publishers.close();
    }

    private CompletableFuture<PubSubMessage> store(String id, PubSubMessage message) {
        if (message == null)  {
            log.error("Could not publish query first. Not storing it {}", message);
            return NONE;
        }
        // TODO: consider sending a kill if an exception happens here. It's technically a leak to the backend
        return storage.putObject(id, message).thenComposeAsync(result -> sendKillIfNecessary(result, id, message));
    }

    private CompletableFuture<PubSubMessage> publish(PubSubMessage message) {
        Publisher publisher = publishers.get();
        try {
            PubSubMessage sent = publisher.send(message);
            return CompletableFuture.completedFuture(sent);
        } catch (Exception e) {
            log.error("Unable to publish message", e);
            return NONE;
        }
    }

    private PubSubMessage killQuery(String id) {
        Publisher publisher = publishers.get();
        PubSubMessage message = new PubSubMessage(id, Metadata.Signal.KILL);
        try {
            log.debug("Sending kill signal for {}", id);
            return publisher.send(message);
        } catch (Exception e) {
            log.error("Could not send message {}", message);
            log.error("Error: ", e);
            return null;
        }
    }

    private CompletableFuture<PubSubMessage> sendKillIfNecessary(Boolean status, String id, PubSubMessage message) {
        if (!status) {
            log.error("Error while trying to store query after submitting. Sending a kill for it...");
            log.error("Sending a kill signal for {}", id);
            return send(id, Metadata.Signal.KILL).thenApply(d -> null);
        }
        return CompletableFuture.completedFuture(message);
    }

    private PubSubMessage onSubmit(String id, PubSubMessage message) {
        if (message != null) {
            log.debug("Successfully submitted message for {}", id);
            return message;
        } else {
            log.error("Could not submit message for {}", id);
            return null;
        }
    }

    private static PubSubMessage onSubmitFail(Throwable error, String id) {
        log.error("Failed to submit query {} due to failures in storing or publishing the query", id);
        log.error("Received exception", error);
        return null;
    }

    private static void onStoredMessageRemove(PubSubMessage message) {
        log.debug("Removed message {} from storage", message);
    }

    private Void onStoredMessageRemoveFail(Throwable e, String id) {
        log.error("Exception while trying to remove stored message", e);
        log.error("Could not remove {} from storage", id);
        return null;
    }

    private Void onRespondFail(Throwable e, String id, PubSubMessage response) {
        log.error("Exception while trying to remove stored message", e);
        log.error("Could not remove {} from storage upon receiving {}", id, response);
        return null;
    }
}
