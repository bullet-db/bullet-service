package com.yahoo.bullet.rest.query;

import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.rest.model.WebSocketResponse;
import com.yahoo.bullet.rest.service.WebSocketService;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessageType;

/**
 * Query handler that implements results for Websocket - multiple results per query.
 */
public class WebSocketQueryHandler extends QueryHandler {
    private WebSocketService webSocketService;
    private String sessionID;
    private SimpMessageHeaderAccessor headerAccessor;

    public WebSocketQueryHandler(WebSocketService webSocketService, String sessionID) {
        this.webSocketService = webSocketService;
        this.sessionID = sessionID;
        headerAccessor = SimpMessageHeaderAccessor.create(SimpMessageType.MESSAGE);
        headerAccessor.setSessionId(sessionID);
        headerAccessor.setLeaveMutable(true);
    }

    @Override
    public void complete() {
        super.complete();
        webSocketService.removeSessionID(sessionID);
    }

    @Override
    public void send(PubSubMessage response) {
        if (!isComplete()) {
            WebSocketResponse responseMessage = new WebSocketResponse();
            responseMessage.setType(WebSocketResponse.ResponseType.CONTENT);
            responseMessage.setContent(response.asJSON());
            webSocketService.sendResponse(sessionID, responseMessage, headerAccessor);
        }
    }

    @Override
    public void fail(QueryError cause) {
        if (!isComplete()) {
            WebSocketResponse responseMessage = new WebSocketResponse();
            responseMessage.setType(WebSocketResponse.ResponseType.FAIL);
            responseMessage.setContent(cause.toString());
            webSocketService.sendResponse(sessionID, responseMessage, headerAccessor);
            complete();
        }
    }
}

