package com.yahoo.bullet.rest.controller;

import com.yahoo.bullet.rest.model.WebSocketRequest;
import com.yahoo.bullet.rest.model.WebSocketResponse;
import com.yahoo.bullet.rest.query.WebSocketQueryHandler;
import com.yahoo.bullet.rest.service.QueryService;
import com.yahoo.bullet.rest.service.WebSocketService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;

import java.security.Principal;
import java.util.UUID;

@Controller
public class WebSocketController {
    @Autowired
    private WebSocketService webSocketService;

    @MessageMapping("/submit.request")
    public void submitRequest(@Payload WebSocketRequest request,
                              SimpMessageHeaderAccessor headerAccessor) {
        if (request.getType() == WebSocketRequest.RequestType.NEW_QUERY) {
            webSocketService.submitQuery(request.getContent(), headerAccessor);
        }
    }
}
