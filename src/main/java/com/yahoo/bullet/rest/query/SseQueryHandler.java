package com.yahoo.bullet.rest.query;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.rest.service.QueryService;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;

public class SseQueryHandler extends QueryHandler {
    private SseEmitter emitter;
    private String queryID;
    private QueryService queryService;

    public SseQueryHandler(String queryID, SseEmitter emitter, QueryService queryService) {
        this.queryID = queryID;
        this.emitter = emitter;
        this.queryService = queryService;
    }

    @Override
    public void complete() {
        super.complete();
        emitter.complete();
    }

    @Override
    public void send(PubSubMessage response) {
        if (!isComplete()) {
            try {
                emitter.send(response.getContent());
            } catch (IOException e) {
                queryService.submitSignal(queryID, Metadata.Signal.KILL);
                complete();
            }
        }
    }

    @Override
    public void fail(QueryError cause) {
        if (!isComplete()) {
            try {
                emitter.send(cause.toString());
            } catch (IOException e) {
                queryService.submitSignal(queryID, Metadata.Signal.KILL);
            }
            complete();
        }
    }
}
