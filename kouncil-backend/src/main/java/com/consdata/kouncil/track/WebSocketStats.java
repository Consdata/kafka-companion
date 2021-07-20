package com.consdata.kouncil.track;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class WebSocketStats {
    private String wsSession;
    private String taskScheduler;
    private String clientInbound;
    private String clientOutbound;
    private Map<String, String> destinations;
}
