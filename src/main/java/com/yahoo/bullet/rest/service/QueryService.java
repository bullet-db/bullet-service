/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.service;

import com.yahoo.bullet.common.metrics.MetricCollector;
import com.yahoo.bullet.common.metrics.MetricPublisher;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.PubSubMessageSerDe;
import com.yahoo.bullet.pubsub.PubSubResponder;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.rest.common.Metrizable;
import com.yahoo.bullet.rest.common.PublisherRandomPool;
import com.yahoo.bullet.rest.common.Reader;
import com.yahoo.bullet.rest.common.Utils;
import com.yahoo.bullet.storage.StorageManager;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.PreDestroy;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Slf4j
public class QueryService extends PubSubResponder implements Metrizable {
    private final StorageManager<PubSubMessage> storage;
    private final List<PubSubResponder> responders;
    private final PublisherRandomPool publishers;
    private final List<Reader> readers;
    private final PubSubMessageSerDe sendSerDe;
    @Getter
    private final boolean metricEnabled;
    @Getter
    private final MetricPublisher metricPublisher;
    @Getter
    private final MetricCollector metricCollector;

    private static final CompletableFuture<PubSubMessage> NONE = CompletableFuture.completedFuture(null);

    static final String QUERY_SUBMIT_SUCCESS = "query.pubsub.submit.success";
    static final String QUERY_SUBMIT_FAIL = "query.pubsub.submit.fail";
    static final String QUERY_ADD_SUCCESS = "query.storage.add.success";
    static final String QUERY_ADD_FAIL = "query.storage.add.fail";
    static final String QUERY_REMOVE_SUCCESS = "query.storage.remove.success";
    static final String QUERY_REMOVE_FAIL = "query.storage.remove.fail";
    static final String QUERY_CLEANUP_SUCCESS = "query.storage.cleanup.success";
    static final String QUERY_CLEANUP_FAIL = "query.storage.cleanup.fail";
    static final String QUERY_RETRIEVE_SUCCESS = "query.storage.retrieve.success";
    static final String QUERY_RETRIEVE_FAIL = "query.storage.retrieve.fail";
    static final String QUERY_KILL_SUCCESS = "query.pubsub.kill.success";
    static final String QUERY_KILL_FAIL = "query.pubsub.kill.fail";
    static final String QUERY_RESPONSE_SUCCESS = "query.storage.response.success";
    static final String QUERY_RESPONSE_FAIL = "query.storage.response.fail";

    private static final List<String> METRICS =
        Arrays.asList(QUERY_SUBMIT_SUCCESS, QUERY_SUBMIT_FAIL, QUERY_ADD_SUCCESS, QUERY_ADD_FAIL, QUERY_REMOVE_SUCCESS,
                      QUERY_REMOVE_FAIL, QUERY_CLEANUP_SUCCESS, QUERY_CLEANUP_FAIL, QUERY_RETRIEVE_SUCCESS,
                      QUERY_RETRIEVE_FAIL, QUERY_KILL_SUCCESS, QUERY_KILL_FAIL, QUERY_RESPONSE_SUCCESS, QUERY_RESPONSE_FAIL);


    /**
     * Constructor that takes various necessary components.
     *
     * @param storageManager The non-null {@link StorageManager} to use.
     * @param responders The non-empty {@link List} of {@link PubSubResponder} to use.
     * @param publishers The non-empty {@link List} of {@link Publisher} to use.
     * @param subscribers The non-empty {@link List} of {@link Subscriber} to use.
     * @param pubSubMessageSendSerDe The {@link PubSubMessageSerDe} to use for sending messages to the PubSub.
     * @param sleep The time to sleep between checking for messages from the pubsub.
     * @param metricPublisher The optional {@link MetricPublisher} to use to report metrics. Can be null.
     */
    public QueryService(StorageManager<PubSubMessage> storageManager, List<PubSubResponder> responders,
                        List<Publisher> publishers, List<Subscriber> subscribers,
                        PubSubMessageSerDe pubSubMessageSendSerDe, int sleep, MetricPublisher metricPublisher) {
        super(null);
        Objects.requireNonNull(storageManager);
        Objects.requireNonNull(responders);
        Objects.requireNonNull(pubSubMessageSendSerDe);
        Utils.checkNotEmpty(publishers);
        Utils.checkNotEmpty(subscribers);
        this.metricEnabled = metricPublisher != null;
        this.metricPublisher = metricPublisher;
        this.metricCollector = new MetricCollector(METRICS);
        this.storage = storageManager;
        this.responders = responders;
        this.sendSerDe = pubSubMessageSendSerDe;
        this.publishers = new PublisherRandomPool(publishers);
        this.readers = subscribers.stream().map(x -> new Reader(x, this, sleep)).collect(Collectors.toList());
        this.readers.forEach(Reader::start);
    }

    /**
     * Submit a query to Bullet and store it in the storage. Unless the publishing succeeds, the query is not stored.
     *
     * @param id The query ID of the query.
     * @param query The query to send.
     * @param queryString The string representation of the query.
     * @return A {@link CompletableFuture} that resolves to the sent {@link PubSubMessage} or null if it could not be sent.
     */
    public CompletableFuture<PubSubMessage> submit(String id, Query query, String queryString) {
        log.debug("Submitting query {}", id);
        PubSubMessage message = sendSerDe.toMessage(id, query, queryString);
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
        CompletableFuture<PubSubMessage> removed = storage.remove(id);
        return removed.thenAccept(this::onStoredMessageRemove)
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
            CompletableFuture<PubSubMessage> removed = storage.remove(id);
            removed.thenAccept(this::onStoredMessageRemoveForResponseSuccess)
                   .exceptionally(e -> onStoredMessageRemoveForResponseFail(e, id, response));
        }
        for (PubSubResponder responder : responders) {
            try {
                responder.respond(id, response);
            } catch (Exception e) {
                log.error("Error while responding for {} using {}", id, responder);
                this.incrementMetric(QUERY_RESPONSE_FAIL);
            }
        }
        this.incrementMetric(QUERY_RESPONSE_SUCCESS);
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
        return publish(sendSerDe.toMessage(new PubSubMessage(id, signal)));
    }

    /**
     * Sends a {@link PubSubMessage} to Bullet without storing it. This can be used to send signals or anything else.
     *
     * @param message The non-null {@link PubSubMessage} to send.
     * @return A {@link CompletableFuture} that resolves to the sent {@link PubSubMessage} or null if it could not be sent.
     */
    public CompletableFuture<PubSubMessage> send(PubSubMessage message) {
        Objects.requireNonNull(message);
        return publish(sendSerDe.toMessage(message));
    }

    /**
     * Retrieves the stored {@link PubSubMessage} of a submitted query.
     *
     * @param id The non-null ID of the query.
     * @return A {@link CompletableFuture} that resolves to the stored {@link PubSubMessage} or null if it could not be found.
     */
    public CompletableFuture<PubSubMessage> get(String id) {
        return storage.get(id)
                      .thenApply(this::onStoredMessageRetrieve)
                      .exceptionally(e -> onStoredMessageRetrieveFail(e, id));
    }

    /**
     * Fires and forgets the metrics using the publisher.
     */
    @Scheduled(fixedDelayString = "${bullet.metric.publish.interval.ms}")
    public void publishMetrics() {
        if (metricEnabled) {
            metricPublisher.fire(metricCollector.extractMetrics());
        }
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
        return storage.put(id, message).thenComposeAsync(result -> onStore(result, id, message));
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
            PubSubMessage sent = publisher.send(sendSerDe.toMessage(message));
            this.incrementMetric(QUERY_KILL_SUCCESS);
            return sent;
        } catch (Exception e) {
            log.error("Could not send message {}", message);
            log.error("Error: ", e);
            this.incrementMetric(QUERY_KILL_FAIL);
            return null;
        }
    }

    private CompletableFuture<PubSubMessage> onStore(Boolean status, String id, PubSubMessage message) {
        if (!status) {
            log.error("Error while trying to store query after submitting. Sending a kill for it...");
            log.error("Sending a kill signal for {}", id);
            this.incrementMetric(QUERY_ADD_FAIL);
            return send(id, Metadata.Signal.KILL).thenApply(d -> null);
        }
        this.incrementMetric(QUERY_ADD_SUCCESS);
        return CompletableFuture.completedFuture(message);
    }

    private PubSubMessage onSubmit(String id, PubSubMessage message) {
        if (message != null) {
            log.debug("Successfully submitted message for {}", id);
            this.incrementMetric(QUERY_SUBMIT_SUCCESS);
            return message;
        } else {
            log.error("Could not submit message for {}", id);
            this.incrementMetric(QUERY_SUBMIT_FAIL);
            return null;
        }
    }

    private PubSubMessage onSubmitFail(Throwable error, String id) {
        log.error("Failed to submit query {} due to failures in storing or publishing the query", id);
        log.error("Received exception", error);
        this.incrementMetric(QUERY_SUBMIT_FAIL);
        return null;
    }

    private PubSubMessage onStoredMessageRetrieve(PubSubMessage message) {
        log.debug("Retrieved message {} from storage", message);
        this.incrementMetric(QUERY_RETRIEVE_SUCCESS);
        return sendSerDe.fromMessage(message);
    }

    private PubSubMessage onStoredMessageRetrieveFail(Throwable e, String id) {
        log.error("Exception while trying to retrieve stored message", e);
        log.error("Could not retrieve {} from storage", id);
        this.incrementMetric(QUERY_RETRIEVE_FAIL);
        return null;
    }

    private void onStoredMessageRemove(PubSubMessage message) {
        log.debug("Removed message {} from storage", message);
        this.incrementMetric(QUERY_REMOVE_SUCCESS);
    }

    private Void onStoredMessageRemoveFail(Throwable e, String id) {
        log.error("Exception while trying to remove stored message", e);
        log.error("Could not remove {} from storage", id);
        this.incrementMetric(QUERY_REMOVE_FAIL);
        return null;
    }

    private Void onStoredMessageRemoveForResponseSuccess(PubSubMessage message) {
        log.debug("Removed message {} from storage for response", message);
        this.incrementMetric(QUERY_CLEANUP_SUCCESS);
        return null;
    }

    private Void onStoredMessageRemoveForResponseFail(Throwable e, String id, PubSubMessage response) {
        log.error("Exception while trying to remove stored message", e);
        log.error("Could not remove {} from storage upon receiving {}", id, response);
        this.incrementMetric(QUERY_CLEANUP_FAIL);
        return null;
    }
}
