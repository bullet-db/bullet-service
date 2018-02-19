/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.rest.pubsub;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import com.yahoo.bullet.pubsub.service.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MemoryPubSub extends PubSub {

    public MemoryPubSub(BulletConfig config) throws PubSubException {
        super(config);
//        queryTopicName = getRequiredConfig(String.class, KafkaConfig.REQUEST_TOPIC_NAME);
//        responseTopicName  = getRequiredConfig(String.class, KafkaConfig.RESPONSE_TOPIC_NAME);
//        topic = (context == PubSub.Context.QUERY_PROCESSING) ? queryTopicName : responseTopicName;
//
//        queryPartitions = parsePartitionsFor(queryTopicName, KafkaConfig.REQUEST_PARTITIONS);
//        responsePartitions = parsePartitionsFor(responseTopicName, KafkaConfig.RESPONSE_PARTITIONS);
//        partitions = (context == PubSub.Context.QUERY_PROCESSING) ? queryPartitions : responsePartitions;
    }

    @Override
    public Publisher getPublisher() throws PubSubException {
        //Map<String, Object> properties = getProperties(PRODUCER_NAMESPACE, KAFKA_PRODUCER_PROPERTIES);
        //KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);

        if (context == PubSub.Context.QUERY_PROCESSING) {
            // We don't need to provide topic-partitions here since they should be in the message metadata
            //return new MemoryResponsePublisher();
            return new com.yahoo.bullet.pubsub.service.MemoryPublisher(config, "http://localhost:9999/api/bullet/pubsub/publish/response");
        }

        return new MemoryQueryPublisher(config);

//        List<TopicPartition> to = (queryPartitions == null) ? getAllPartitions(getDummyProducer(), queryTopicName) : queryPartitions;
//        List<TopicPartition> from = (responsePartitions == null) ? getAllPartitions(getDummyProducer(), responseTopicName) : responsePartitions;
//
//        return new KafkaQueryPublisher(producer, to, from);
    }

    @Override
    public List<Publisher> getPublishers(int n) throws PubSubException {
        // Kafka Publishers are thread safe and can be reused
        return Collections.nCopies(n, getPublisher());
    }

    @Override
    public Subscriber getSubscriber() throws PubSubException {
        // return getSubscriber(partitions, topic);

        if (context == Context.QUERY_PROCESSING) {
            return new MemoryQuerySubscriber();
        }
        return new MemoryResponseSubscriber();
    }

    @Override
    public List<Subscriber> getSubscribers(int n) throws PubSubException {
        List<Subscriber> subscribers = new ArrayList<>();
        if (context == Context.QUERY_PROCESSING) {
            for (int i = 0; i < n; i++) {
                subscribers.add(new MemoryQuerySubscriber());
            }
        } else {
            for (int i = 0; i < n; i++) {
                subscribers.add(new MemoryResponseSubscriber());
            }
        }
        return subscribers;
    }

}
